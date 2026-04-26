package experiments.suite.parameter.output;

import experiments.suite.parameter.model.ParameterExperimentRow;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class ParameterSummaryWriter {
    private ParameterSummaryWriter() {
    }

    public static void write(Path outputFile, List<ParameterExperimentRow> rows) throws IOException {
        if (outputFile.getParent() != null) {
            Files.createDirectories(outputFile.getParent());
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile.toFile()))) {
            writer.write("dataset,study,resolution,min_traj,rc,avg_latency_ms,avg_rki,avg_ct,query_count");
            writer.newLine();
            for (ParameterExperimentRow row : rows) {
                writer.write(row.toCsvRow());
                writer.newLine();
            }
        }
    }
}
