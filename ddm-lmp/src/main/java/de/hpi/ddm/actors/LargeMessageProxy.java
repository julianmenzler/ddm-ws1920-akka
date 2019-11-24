package de.hpi.ddm.actors;

import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import akka.stream.ActorMaterializer;
import akka.stream.SourceRef;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamRefs;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(BytesMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) throws IOException {
		// Configure proxy actors
		ActorSelection receiverProxy = this.context().actorSelection(message.receiver.path().child(DEFAULT_NAME));

		// Serialization
		Byte[] bytes = ArrayUtils.toObject(serialize(message.message));

		// Configure data stream with serialized data
		final Source<Byte, NotUsed> messageSource = Source.from(Arrays.asList(bytes));
		final SourceRef<Byte> messageRefs = messageSource.runWith(StreamRefs.sourceRef(), this.context().system());

		// Serialize and stream to other proxy actor
		BytesMessage bytesMessage = new BytesMessage(messageRefs, this.sender(), message.receiver);
		receiverProxy.tell(bytesMessage, this.self());
	}

	private byte[] serialize(Object obj) throws IOException {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		Output output = new Output(outputStream);
        new Kryo().writeClassAndObject(output, obj);
		output.close();
		byte[] buffer = outputStream.toByteArray();
		outputStream.close();
		return buffer;
	}

	private Object deserialize(byte[] bytes) {
		return new Kryo().readClassAndObject(new Input(new ByteArrayInputStream(bytes)));
	}

	private void handle(BytesMessage message) {
		// Collect the bytes with Sink.seq and move them into a list to be serialized
		final SourceRef<Byte> sourceRef = message.bytes;
		final Source<Byte, NotUsed> source = sourceRef.getSource();
		final CompletionStage<List<Byte>> bytesCompletion = source.runWith(Sink.seq(), this.context().system());

		// Deserialize and forward to receiver
        bytesCompletion.thenAccept(result -> {
            byte[] bytes = ArrayUtils.toPrimitive(result.toArray(new Byte[result.size()]));
            Object object = this.deserialize(bytes);
            message.receiver.tell(object, message.sender);
        });
	}
}
