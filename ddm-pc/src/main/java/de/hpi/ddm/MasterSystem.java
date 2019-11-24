package de.hpi.ddm;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import de.hpi.ddm.actors.*;
import de.hpi.ddm.configuration.Configuration;
import de.hpi.ddm.configuration.ConfigurationSingleton;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class MasterSystem {
	
	public static final String MASTER_ROLE = "master";

	public static void start() {
		final Configuration cfg = ConfigurationSingleton.get();
		
		final Config config = ConfigFactory.parseString(
				"akka.remote.artery.canonical.hostname = \"" + cfg.getHost() + "\"\n" +
				"akka.remote.artery.canonical.port = " + cfg.getPort() + "\n" +
				"akka.cluster.roles = [" + MASTER_ROLE + "]\n" +
				"akka.cluster.seed-nodes = [\"akka://" + cfg.getActorSystemName() + "@" + cfg.getHost() + ":" + cfg.getPort() + "\"]")
			.withFallback(ConfigFactory.load("application"));
		
		final ActorSystem system = ActorSystem.create(cfg.getActorSystemName(), config);

	//	ActorRef clusterListener = system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
	//	ActorRef metricsListener = system.actorOf(MetricsListener.props(), MetricsListener.DEFAULT_NAME);
		
		ActorRef reaper = system.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);
		
		ActorRef reader = system.actorOf(Reader.props(), Reader.DEFAULT_NAME);
		
		ActorRef collector = system.actorOf(Collector.props(), Collector.DEFAULT_NAME);

		ActorRef dispatcher = system.actorOf(Dispatcher.props(), Dispatcher.DEFAULT_NAME);

		ActorRef master = system.actorOf(Master.props(reader, dispatcher, collector), Master.DEFAULT_NAME);

		Cluster.get(system).registerOnMemberUp(new Runnable() {
			@Override
			public void run() {

				for (int i = 0; i < cfg.getNumWorkers(); i++)
					system.actorOf(Worker.props(), Worker.DEFAULT_NAME + i);

				if (!cfg.isStartPaused())
					system.actorSelection("/user/" + Master.DEFAULT_NAME).tell(new Master.StartMessage(), ActorRef.noSender());
			}
		});

		Cluster.get(system).registerOnMemberRemoved(new Runnable() {
			@Override
			public void run() {
				system.terminate();

				new Thread() {
					@Override
					public void run() {
						try {
							Await.ready(system.whenTerminated(), Duration.create(10, TimeUnit.SECONDS));
						} catch (Exception e) {
							System.exit(-1);
						}
					}
				}.start();
			}
		});
		
		if (cfg.isStartPaused()) {
			System.out.println("Press <enter> to start!");
			try (final Scanner scanner = new Scanner(System.in)) {
				scanner.nextLine();
			}
			
			system.actorSelection("/user/" + Master.DEFAULT_NAME).tell(new Master.StartMessage(), ActorRef.noSender());
		}
	}
}
