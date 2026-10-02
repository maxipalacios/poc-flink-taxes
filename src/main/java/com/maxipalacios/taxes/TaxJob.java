package com.maxipalacios.taxes;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public final class TaxJob {

    private TaxJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10_000);

        // Placeholder: Flink only executes a topology once at least one
        // operator is attached downstream, so this idle source (which emits
        // nothing) feeds a discarding sink purely to keep the job RUNNING.
        // The CDC pipeline (upcoming work) will replace this graph.
        env.addSource(new IdlePlaceholderSource())
                .name("idle-placeholder")
                .uid("idle-placeholder")
                .sinkTo(new DiscardingSink<>())
                .name("discard-placeholder")
                .uid("discard-placeholder");

        env.execute("poc-flink-taxes");
    }

    /** Emits nothing; exists only so the topology is non-empty and stays RUNNING. */
    @SuppressWarnings("deprecation")
    private static final class IdlePlaceholderSource implements SourceFunction<Void> {

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Void> ctx) throws Exception {
            while (running) {
                Thread.sleep(1_000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
