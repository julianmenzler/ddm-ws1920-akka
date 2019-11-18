package de.hpi.ddm.actors;

import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.io.DirectByteBufferPool;
import akka.serialization.*;
import akka.stream.ActorMaterializer;
import akka.stream.SourceRef;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.SourceQueueWithComplete;
import akka.stream.javadsl.StreamRefs;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;

import java.util.ArrayList;
import java.util.List;
import com.twitter.chill.SerDeState;
import de.hpi.ddm.structures.KryoPoolSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;
import scala.util.control.Exception;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CompletionStage;

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
	public static class BytesMessage implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private SourceRef<Byte> bytes;
		private int serializerId;
		private String serializerManifest;
		private String senderIdentifier;
		private String receiverIdentifier;
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

	private final Serialization serialization = SerializationExtension.get(this.context().system());
	private final ActorMaterializer mat = ActorMaterializer.create(this.context().system());
	private final int MAX_ALLOWED_SIZE = 20000000;

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(BytesMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {
		// Configure proxy actors
		ActorRef receiver = message.receiver;
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));
        String senderActorIdentifier = Serialization.serializedActorPath(this.sender());
		String receiverActorIdentifier = Serialization.serializedActorPath(message.receiver);

		// Serialization
		int serializerId = serialization.findSerializerFor(message).identifier();
		String manifest = Serializers.manifestFor(serialization.findSerializerFor(message), message);
		byte[] b = this.serialization.serialize(message.message).get();
		List<Byte> bytes = new ArrayList<>();

		// Configure data stream
		final SourceQueueWithComplete<Byte> sourceQueue =
				Source.queue(1024, akka.stream.OverflowStrategy.backpressure())
						.runWith((Sink<Byte, NotUsed>)Sink.ignore(), this.mat);
		Source<Byte, NotUsed> messageSource = Source.from(bytes);
		final SourceRef<Byte> messageSourceRef =
				messageSource.map(sourceQueue.offer)
						.runWith(StreamRefs.sourceRef(), this.context().system());

		BytesMessage byteMessage = new BytesMessage(
				messageSourceRef,
				serializerId,
				manifest,
				senderActorIdentifier,
				receiverActorIdentifier);
		receiverProxy.tell(byteMessage, this.self());
	}

	private void handle(BytesMessage message) {
		ActorRef sender = this.context().system().provider().resolveActorRef(message.senderIdentifier);
		ActorRef receiver = this.context().system().provider().resolveActorRef(message.receiverIdentifier);

		// Collect the bytes and collect them in a list to be serialized
		final SourceRef<Byte> sourceRef = message.bytes;
		final Source<Byte, NotUsed> source = sourceRef.getSource();
		final CompletionStage<List<Byte>> bytesCompletion =
				source.limit(MAX_ALLOWED_SIZE)
						.runWith(Sink.seq(), mat);

		// Deserialize and route to receiver actor
		bytesCompletion
				.thenApply((bytes) -> new byte[0])
				.thenApply((bytes) -> this.serialization.deserialize(bytes, message.serializerId, message.serializerManifest).get())
				.thenApply((object) -> receiver.tell(object, sender));
	}
}
