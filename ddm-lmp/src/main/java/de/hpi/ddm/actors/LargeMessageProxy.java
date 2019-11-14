package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.io.DirectByteBufferPool;
import akka.serialization.ByteBufferSerializer;
import akka.serialization.SerializerWithStringManifest;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.twitter.chill.SerDeState;
import de.hpi.ddm.structures.KryoPoolSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	
	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;
	}

	@NoArgsConstructor
	public static class LargeMessageByteBufSerializer extends SerializerWithStringManifest implements ByteBufferSerializer {

		DirectByteBufferPool pool = new akka.io.DirectByteBufferPool( 1024 * 1024, 10);

		@Override
		public int identifier() {
			return 1337;
		}

		@Override
		public String manifest(Object o) {
			return "serialized-" + o.getClass().getSimpleName();
		}

		@Override
		public byte[] toBinary(Object o) {
			ByteBuffer buf = pool.acquire();
			try {
				toBinary(o, buf);
				// flip() makes a buffer ready for a new sequence of channel-write or relative get operations:
				// It sets the limit to the current position and then sets the position to zero.
				buf.flip();
				// Remaining: return the number of elements between the current position and the limit.
				final byte[] bytes = new byte[buf.remaining()];
				buf.get(bytes);
				return bytes;
			} finally {
				pool.release(buf);
			}
		}

		@Override
		public Object fromBinary(byte[] bytes, String manifest) throws NotSerializableException {
			return fromBinary(ByteBuffer.wrap(bytes), manifest);
		}


		@Override
		public void toBinary(Object o, ByteBuffer buf) {
			ByteBufferOutput output = new ByteBufferOutput(buf, 1024);
			SerDeState kryo = KryoPoolSingleton.get().borrow();
			try {
				kryo.writeOutputTo(output);
				kryo.writeObject(o);
				output.flush();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				KryoPoolSingleton.get().release(kryo);
				output.release();
			}
		}

		@Override
		public Object fromBinary(ByteBuffer buf, String manifest) throws NotSerializableException {
			SerDeState kryo = KryoPoolSingleton.get().borrow();
			try {
				ByteBufferInput input = new ByteBufferInput(buf);
				kryo.setInput(input);
				return kryo.readObject(BytesMessage.class);
			} finally {
				KryoPoolSingleton.get().release(kryo);
			}
		}
	}

	/////////////////
	// Actor State //
	/////////////////
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(BytesMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

		// https://doc.akka.io/docs/akka/current/stream/stream-io.html#streaming-file-io
		// SinkAktor

		// This will definitely fail in a distributed setting if the serialized message is large!
		// Solution options:
		// 1. Serialize the object and send its bytes batch-wise (make sure to use artery's side channel then).
		// 2. Serialize the object and send its bytes via Akka streaming.
		// 3. Send the object via Akka's http client-server component.
		// 4. Other ideas ...

		receiverProxy.tell(new BytesMessage<>(message.getMessage(), this.sender(), message.getReceiver()), this.self());
	}

	private void handle(BytesMessage<?> message) {
		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
		message.getReceiver().tell(message.getBytes(), message.getSender());
	}
}
