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

	@Data @AllArgsConstructor
	public static class CollectPasswordMessage implements Serializable {
		private static final long serialVersionUID = 3303011691659723997L;
		private String passwordHash;
		private String password;
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
	private HashMap<String, String> passwords = new HashMap<>();
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
				.match(CollectPasswordMessage.class, this::handle)
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
		if (message.getLines().isEmpty()) {
			return;
		}

		HashMap<String, String> hintHash = getHintHashFromLines(message.getLines());

		this.workers.get(0).tell(new Worker.CreatePermutationsMessage(hintHash, passwordAlphabet), this.self());

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
			worker.tell(new Worker.CrackPasswordMessage(passwordAlphabet, passwordHash, Integer.valueOf(line[3]), clearTextHints), this.self());
		});
	}

	private void handle(CollectPasswordMessage message) {
		this.collector.tell(new Collector.CollectMessage("Found password: " + message.password), this.self());
		passwords.put(message.passwordHash, message.password);

		// All password were found
		if(passwords.size() == lines.size()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.terminate();
		}
	}

	private HashMap<String, String> getHintHashFromLines(List<String[]> lines) {
		HashMap<String, String> hints = new HashMap<>();
		for (String[] line : lines) {
			lines.add(line);
			passwordAlphabet = line[2];
			for (int i = 5; i < line.length; i++) {
				hints.put(line[i], null);
			}
		}

		return hints;
	}
}
