package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.routing.Router;
import akka.routing.SmallestMailboxRoutingLogic;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "master";

    public static Props props(final ActorRef reader, final ActorRef collector, final ActorRef dispatcher) {
        return Props.create(Master.class, () -> new Master(reader, collector, dispatcher));
    }

    public Master(final ActorRef reader, final ActorRef collector, final ActorRef dispatcher) {
        this.reader = reader;
        this.collector = collector;
        this.dispatcher = dispatcher;
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    public static class StartMessage implements Serializable {
        private static final long serialVersionUID = -50374816448627600L;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BatchMessage implements Serializable {
        private static final long serialVersionUID = 8343040942748609598L;
        private List<String[]> lines;
    }

    @Data @NoArgsConstructor
    @AllArgsConstructor
    public static class NewHintsMessage implements Serializable {
        private static final long serialVersionUID = 3303011691659723997L;
        private HashMap<String, String> hints;
    }

    @Data @NoArgsConstructor
    @AllArgsConstructor
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
    private final ActorRef dispatcher;

    private long startTime;

    private HashMap<String, String> hintHashToHint = new HashMap<>(); // Map hint hash -> hint (clear-text)
    private HashMap<String, String> passwordHashToPassword = new HashMap<>(); // Map Password hash -> Password
    private HashMap<String, Set<String>> passwordHashToHintHashes = new HashMap<>(); // Map Password hash -> Hint hashes
    private Integer hintsFound = 0;
    private Integer passwordsFound = 0;
    private Integer passwordLength = 0;
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
                .match(NewHintsMessage.class, this::handle)
                .match(CollectPasswordMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    protected void terminate() {
        this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
        this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
        this.dispatcher.tell(new Dispatcher.StopMessage(), this.sender());

        this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());

        long executionTime = System.currentTimeMillis() - this.startTime;
        this.log().info("Algorithm finished in {} ms", executionTime);
    }


    // Logic – Reader

    protected void handle(StartMessage message) {
        this.startTime = System.currentTimeMillis();

        this.reader.tell(new Reader.ReadMessage(), this.self());
    }

    protected void handle(BatchMessage message) {
        if (message.getLines().isEmpty()) {

            // We want to start the work as soon as we got all lines
            assert(!passwordAlphabet.equals(""));
            // Create HintAlphabets by removing a single char from the passwordAlphabet
            for (int i = 0; i < passwordAlphabet.length(); i++) {
                // Multiple hint alphabets because each char can be removed
                String hintAlphabet = passwordAlphabet.substring(0, i) + passwordAlphabet.substring(i + 1);

                // Start cracking hints - for each hint alphabet
                dispatcher.tell(new Worker.CrackHintsMessage(hintHashToHint.keySet(), hintAlphabet), this.self());
            }
            return;
        }

        storeLinesInfo(message.getLines());

        this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());

        // Request new batch
        this.reader.tell(new Reader.ReadMessage(), this.self());
    }

    // Logic - Worker Results

    private void handle(NewHintsMessage message) {
        hintHashToHint.putAll(message.hints); // Replaces null values for each given key in message.hints
        hintsFound += message.hints.size();

        // We want to start password cracking when all hints are found
        if (hintsFound != hintHashToHint.size()) {
            return;
        }

        // Start password cracking
        passwordHashToPassword.keySet().forEach((passwordHash) -> {
            // Map password hash to the hints we found previously
            Set<String> clearTextHints = new HashSet<>();
            for (String hashedHint : passwordHashToHintHashes.get(passwordHash)) {
                clearTextHints.add(hintHashToHint.get(hashedHint));
            }

            // Start cracking the password using hints
            dispatcher.tell(new Worker.CrackPasswordMessage(passwordAlphabet, passwordHash, passwordLength, clearTextHints), this.self());
        });
    }

    private void handle(CollectPasswordMessage message) {
        this.collector.tell(new Collector.CollectMessage("Found password: " + message.password), this.self());
        passwordHashToPassword.put(message.passwordHash, message.password);
        passwordsFound += 1;

        // All password were found
        if (passwordHashToPassword.size() == passwordsFound) {
            this.collector.tell(new Collector.PrintMessage(), this.self());
            this.terminate();
        }
    }


    ////////////////////
    // Helper funcs   //
    ////////////////////

    private void storeLinesInfo(List<String[]> lines) {
		String passwordHash;

    	for (String[] line : lines) {
            // Store alphabet
            passwordAlphabet = line[2];
            passwordLength = Integer.valueOf(line[3]);
            passwordHash = line[4];

            // Store hint hashes
            Set<String> hintHashes = new HashSet<>();
            for (int i = 5; i < line.length; i++) {
                hintHashes.add(line[i]);
            }
            hintHashes.forEach((hintHash) -> hintHashToHint.put(hintHash, null));
            passwordHashToHintHashes.put(passwordHash, hintHashes);

            // Store password hashes
            passwordHashToPassword.put(passwordHash, null);
        }
    }
}
