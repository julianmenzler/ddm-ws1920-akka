package de.hpi.ddm.actors;

import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializers;
import akka.stream.ActorMaterializer;
import akka.stream.SourceRef;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamRefs;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
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
		private  SourceRef<Byte> bytes;
		private  int serializerId;
		private  String serializerManifest;
		private  ActorRef sender;
		private  ActorRef receiver;
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
		ActorSelection receiverProxy = this.context().actorSelection(message.receiver.path().child(DEFAULT_NAME));

		// Serialization
		int serializerId = serialization.findSerializerFor(message).identifier();
		String manifest = Serializers.manifestFor(serialization.findSerializerFor(message), message);
		Byte[] bytes = ArrayUtils.toObject(this.serialization.serialize(message.message).get());

		// Configure data stream
		final Source<Byte, NotUsed> messageSource = Source.from(Arrays.asList(bytes));
		final CompletionStage<SourceRef<Byte>> messageRefs = messageSource.runWith(StreamRefs.sourceRef(), this.mat);
		messageRefs.thenApply(ref -> new BytesMessage(ref, serializerId, manifest, this.sender(), message.receiver) )
				.thenAccept(result -> receiverProxy.tell(result, this.self()));
	}

	private void handle(BytesMessage message) {
		// Collect the bytes and collect them in a list to be serialized
		final SourceRef<Byte> sourceRef = message.bytes;
		final Source<Byte, NotUsed> source = sourceRef.getSource();
		final CompletionStage<List<Byte>> bytesCompletion = source.runWith(Sink.seq(), mat);

		// Deserialize and route to receiver actor
		bytesCompletion
				.thenApply(bytes -> this.serialization.deserialize((byte[]) ArrayUtils.toPrimitive(bytes.toArray()), message.serializerId, message.serializerManifest).get())
				.thenAccept((object) -> message.receiver.tell(object, message.sender));
	}
}
