package com.maxipalacios.taxes;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class TaxJob {

    private TaxJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10_000);

        // The first CDC pipeline will be implemented in the next step.
        // Keeping this initial job minimal lets us validate the Flink
        // runtime/devcontainer independently from connector configuration.

        env.execute("poc-flink-taxes");
    }
}
