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
import de.hpi.ddm.structures.KryoPoolSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.lang.reflect.Array;
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
		private  Class klass;
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
		Byte[] bytes = ArrayUtils.toObject(this.serialization.serialize(message.message).get());

		// Configure data stream with serialized data
		final Source<Byte, NotUsed> messageSource = Source.from(Arrays.asList(bytes));
		final CompletionStage<SourceRef<Byte>> messageRefs = messageSource.runWith(StreamRefs.sourceRef(), this.mat);

		// Serialize and stream to other proxy actor
        messageRefs.thenApply(ref -> new BytesMessage(ref, message.getClass(), this.sender(), message.receiver) )
				.thenAccept(result -> receiverProxy.tell(result, this.self()));
	}

	private void handle(BytesMessage message) {
		// Collect the bytes with Sink.seq and move them into a list to be serialized
		final SourceRef<Byte> sourceRef = message.bytes;
		final Source<Byte, NotUsed> source = sourceRef.getSource();
		final CompletionStage<List<Byte>> bytesCompletion = source.runWith(Sink.seq(), mat);

		// Deserialize and forward to receiver
        bytesCompletion.thenAccept(result -> {
            byte[] bytes = ArrayUtils.toPrimitive(result.toArray(new Byte[result.size()]));
            Object object = this.serialization.deserialize(bytes, message.klass);
            message.receiver.tell(object, message.sender);
        });
	}
}
