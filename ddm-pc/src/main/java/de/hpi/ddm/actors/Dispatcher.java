package de.hpi.ddm.actors;

import akka.actor.*;
import de.hpi.ddm.actors.Worker.WorkMessage;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.*;

public class Dispatcher extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "dispatcher";

    public static Props props() {
        return Props.create(Dispatcher.class);
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    public static class WorkerRegistrationMessage implements Serializable {
        private static final long serialVersionUID = 3303081601659723997L;
    }

    @Data @AllArgsConstructor
    public static class StopMessage implements Serializable {
        private static final long serialVersionUID = -8330958742629206627L;
    }


    @Data @AllArgsConstructor
    public static class WorkCompletedMessage implements Serializable {
        private static final long serialVersionUID = -6823011111281387872L;
        public enum status {DONE, FALSE, FAILED}
        WorkCompletedMessage() {}
        private status result;
    }

    @Data @AllArgsConstructor
    private static class QueuedWork {
        private Worker.WorkMessage message;
        private ActorRef sender;
    }

    /////////////////
    // Actor State //
    /////////////////

    private final Queue<QueuedWork> unassignedWork = new LinkedList<>();
    private final Queue<ActorRef> idleWorkers = new LinkedList<>();
    private final Map<ActorRef, Worker.WorkMessage> busyWorkers = new HashMap<>();

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
                .match(WorkerRegistrationMessage.class, this::handle)
                .match(Terminated.class, this::handle)
                .match(StopMessage.class, this::handle)
                .match(WorkMessage.class, this::handle)
                .match(WorkCompletedMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    // Handling workers

    protected void handle(WorkerRegistrationMessage message) {
        this.log().info("Registered at Dispatcher {}", this.sender());
        this.context().watch(this.sender());
        this.assign(this.sender());
    }

    private void handle(Terminated message) {
        this.context().unwatch(message.getActor());

        if (!this.idleWorkers.remove(message.getActor())) {
            WorkMessage work = this.busyWorkers.remove(message.getActor());
            if (work != null) {
                this.assign(work);
            }
        }
        this.log().info("Unregistered {}", message.getActor());
    }

    private void handle(WorkCompletedMessage message) {
        ActorRef worker = this.sender();
        WorkMessage work = this.busyWorkers.remove(worker);

        switch (message.getResult()) {
            case DONE:
                // Ignore
                break;
            case FALSE:
                // Ignore
                break;
            case FAILED:
                this.assign(work);
                break;
        }

        this.assign(worker);
    }

    private void assign(ActorRef worker) {
        QueuedWork work = this.unassignedWork.poll();

        if (work == null) {
            this.idleWorkers.add(worker);
            return;
        }

        this.busyWorkers.put(worker, work.message);
        worker.tell(work.message, work.sender);
    }

    // Handling work

    private void handle(WorkMessage message) {
        this.assign(message);
    }

    private void handle(StopMessage message) {
        // remove messages from queue
        this.unassignedWork.clear();

        // stop idle workers
        ActorRef worker = this.idleWorkers.poll();
        while (worker != null) {
            worker.tell(PoisonPill.getInstance(), this.self());
            worker = this.idleWorkers.poll();
        }

        // stop busy workers
        for (ActorRef actorRef : this.busyWorkers.keySet()) {
            worker = actorRef;
            worker.tell(PoisonPill.getInstance(), this.self());
        }

        this.getContext().stop(this.self());
    }

    private void assign(WorkMessage message) {
        this.unassignedWork.add(new QueuedWork(message, this.sender()));

        ActorRef worker = this.idleWorkers.poll();
        if (worker != null) {
            this.assign(worker);
        }
    }

}