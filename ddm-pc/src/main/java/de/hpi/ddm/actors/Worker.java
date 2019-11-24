package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.ddm.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class Worker extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	public Worker() {
		this.cluster = Cluster.get(this.context().system());
	}
	
	////////////////////
	// Actor Messages //
	////////////////////

	@Data @SuppressWarnings("unused")
	public static class WorkMessage implements Serializable {
		private static final long serialVersionUID = -7643194361868862395L;
		private WorkMessage() {}
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class CrackHintsMessage extends WorkMessage implements Serializable {
		private static final long serialVersionUID = 3303081691659723997L;
		private Set<String> hintHashes;
		private String hintAlphabet;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class CrackPasswordMessage extends WorkMessage implements Serializable {
		private static final long serialVersionUID = 3203081691659723997L;
		private String passwordAlphabet;
		private String passwordHash;
		private Integer passwordLength;
		private Set<String> hints;
	}


	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;

	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
		
		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(MemberRemoved.class, this::handle)
				.match(CrackHintsMessage.class, this::handle)
				.match(CrackPasswordMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void register(Member member) {
		if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
			this.masterSystem = member;
			
			this.getContext()
				.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new Master.WorkerRegistrationMessage(), this.self());
		}
	}
	
	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}

	private void handle(CrackHintsMessage message) {
		HashMap<String, String> hints = new HashMap<>();

		heapPermutation(message.hintAlphabet.toCharArray(), message.hintAlphabet.length(), (permutation) -> {
			// Check if we can find the new and hashed permutation in our hints
			// If yes, we want to save this permutation
			String hashResult = this.getHash(permutation);
			if(message.hintHashes.contains(hashResult)) {
				hints.put(hashResult, permutation);
			}
		});

		this.sender().tell(new Master.NewHintsMessage(hints), this.self());
	}

	private void handle(CrackPasswordMessage message) {
		Set<Character> passwordAlphabet = determinePasswordAlphabetFromHints(message.passwordAlphabet, message.hints);
		String password = crackPassword(passwordAlphabet, "", message.passwordLength,
				(potentialPassword) -> getHash(potentialPassword).equals(message.passwordHash));
		this.sender().tell(new Master.CollectPasswordMessage(message.passwordHash, password), this.self());
	}

	////////////////////
	// Helper stuff   //
	////////////////////

	private interface Cracker {
		boolean checkHash(String password);
	}

	private interface PermutationCallback {
		void call(String permutation);
	}

	private Set<Character> determinePasswordAlphabetFromHints(String passwordAlphabet, Set<String> hints) {
		HashSet<Character> realPasswordAlphabet = new HashSet<>();
		for(Character character : passwordAlphabet.toCharArray()) {
			realPasswordAlphabet.add(character);
		}

		for(String hint : hints) {
			for(Character character : passwordAlphabet.toCharArray()) {
				if(!hint.contains(character.toString())) {
					realPasswordAlphabet.remove(character);
					break;
				}
			}
		}

		return realPasswordAlphabet;
	}

	private String crackPassword(Set<Character> alphabet, String prefix, int k, Cracker cracker) {
		if (k == 0) {
			if (cracker.checkHash(prefix)) {
				// We found the password!
				return prefix;
			} else {
				return null;
			}
		}

		for (Character character : alphabet) {
			String newPrefix = prefix + character;
			String password = crackPassword(alphabet, newPrefix, k - 1, cracker);
			if (password != null) {
				return password;
			}
		}

		return null;
	}

	private String getHash(String line) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes("UTF-8"));
			
			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < hashedBytes.length; i++) {
				stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	// Generating all permutations of an array using Heap's Algorithm
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	private void heapPermutation(char[] a, int size, PermutationCallback callback) {
		// If size is 1, store the obtained permutation
		if (size == 1)
			callback.call(new String(a));

		for (int i = 0; i < size; i++) {
			heapPermutation(a, size - 1, callback);

			// If size is odd, swap first and last element
			if (size % 2 == 1) {
				char temp = a[0];
				a[0] = a[size - 1];
				a[size - 1] = temp;
			}

			// If size is even, swap i-th and last element
			else {
				char temp = a[i];
				a[i] = a[size - 1];
				a[size - 1] = temp;
			}
		}
	}
}