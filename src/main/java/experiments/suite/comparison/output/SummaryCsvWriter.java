package experiments.suite.comparison.output;

import experiments.suite.comparison.model.ExperimentResult;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class SummaryCsvWriter {
    private SummaryCsvWriter() {
    }

    public static void write(Path outputFile, List<ExperimentResult> results) throws IOException {
        if (outputFile.getParent() != null) {
            Files.createDirectories(outputFile.getParent());
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile.toFile()))) {
            writer.write("dataset,distribution,query_range,method,avg_latency_ms,avg_rki,avg_ct,avg_final,query_count");
            writer.newLine();
            for (ExperimentResult result : results) {
                writer.write(result.toCsvRow());
                writer.newLine();
            }
        }
    }
}
