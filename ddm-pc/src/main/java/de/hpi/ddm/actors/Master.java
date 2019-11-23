package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "master";

	public static Props props(final ActorRef reader, final ActorRef collector) {
		return Props.create(Master.class, () -> new Master(reader, collector));
	}

	public Master(final ActorRef reader, final ActorRef collector) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<String[]> lines;
	}

	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}

	@Data @AllArgsConstructor
	public static class FoundHintsMessage implements Serializable {
		private static final long serialVersionUID = 3303011691659723997L;
		private HashMap<String, String> hints;
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;

	private long startTime;

	private List<String[]> lines = new ArrayList<>(); // from CSV
	private HashMap<String, String> hints = new HashMap<>();
	private String passwordAlphabet = "";

	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StartMessage.class, this::handle)
				.match(BatchMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(FoundHintsMessage.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}


	protected void terminate() {
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.workers.add(this.sender());
		this.log().info("Registered {}", this.sender());
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
		this.log().info("Unregistered {}", message.getActor());
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();

		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	protected void handle(BatchMessage message) {

		///////////////////////////////////////////////////////////////////////////////////////////////////////
		// The input file is read in batches for two reasons: /////////////////////////////////////////////////
		// 1. If we distribute the batches early, we might not need to hold the entire input data in memory. //
		// 2. If we process the batches early, we can achieve latency hiding. /////////////////////////////////
		// TODO: Implement the processing of the data for the concrete assignment. ////////////////////////////
		///////////////////////////////////////////////////////////////////////////////////////////////////////

		// [2, Jackson, ABCDEFGHIJK, 10, c178ef3bd2dbf4e92291a9b563c0ae2ca2a8aead62db92fa4d1babe19e5d34d7,
		// 7624e76e72b526d8f7538e6bb96c55760e4483f5993a8068121382e74ddec62d,
		// 834d255d02760d6089edbd564604513563449e77d2cab6009c027f7376d4ca30,
		// b2e939a89b7863100fad643254c524559cc71a4ba49c50402bd04c8446e76130,
		// 0f0c2aefcfcf4b3472529c5af7320aa19b66fe7bb7d9f10e55c9f777f10462bd,
		// d22b58963201e375c51064bfb097e2a05db3504d5da2313b8e926416cc23d743,
		// 0066eb98a0f3fb992b33f95d69338984b0d4ff33101aab05932fd48d72263a9a,
		// 21b5a6f0b9c1531690747cb715ee3ad52c045e8f8f0ec9daf2bca5d574fb4870,
		// 49dc00059541199f160e9f4f8abc7f4951fcf1a1f0976b3982a9fae6f106bb58,
		// 0598a2e796da86752c7745783eba8919c2c8e63c2a23ea9b103d7e797f92ca45]

		if (message.getLines().isEmpty()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
//			this.terminate();
			return;
		}

		// Hint Hash => Cleartext hint
		// Initial: ca70f765d8c1b9b7a2162b19ea8e2b410166840f67ee276d297d0ab3bc05f425 => null
		// Found: ca70f765d8c1b9b7a2162b19ea8e2b410166840f67ee276d297d0ab3bc05f425 => ANBBDFDJDS
		HashMap<String, String> emptyHints = new HashMap<>();
		ActorRef worker = workers.get(0);
		for (String[] line : message.getLines()) {
			lines.add(line);
			passwordAlphabet = line[2];
			for (int i = 5; i < line.length; i++) {
				emptyHints.put(line[i], null);
			}
		}

		Worker.CreatePermutationsMessage createPermutationsMessage = new Worker.CreatePermutationsMessage(emptyHints, passwordAlphabet);
		this.workers.get(0).tell(createPermutationsMessage, this.self());

		this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());

		// Request new batch
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	private void handle(FoundHintsMessage message) {
		ActorRef worker = workers.get(0);

		lines.forEach((line) -> {
			List<String> clearTextHints = new ArrayList<>();
			for (int i = 5; i < line.length; i++) {
				String hint = message.hints.get(line[i]);
				clearTextHints.add(hint);
			}
			String passwordHash = line[4];
			worker.tell(new Worker.CrackPasswordMessage(passwordAlphabet, passwordHash, clearTextHints), this.self());
		});
	}
}
