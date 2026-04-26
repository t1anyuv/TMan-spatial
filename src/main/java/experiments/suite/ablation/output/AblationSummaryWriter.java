package experiments.suite.ablation.output;

import experiments.suite.ablation.model.AblationResultRow;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class AblationSummaryWriter {
    private AblationSummaryWriter() {
    }

    public static void write(Path outputFile, List<AblationResultRow> rows) throws IOException {
        if (outputFile.getParent() != null) {
            Files.createDirectories(outputFile.getParent());
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile.toFile()))) {
            writer.write("variant,order_source,adaptive_partition,resolution,min_traj,avg_latency_ms,avg_rki,avg_ct,avg_final,query_count");
            writer.newLine();
            for (AblationResultRow row : rows) {
                writer.write(row.toCsvRow());
                writer.newLine();
            }
        }
    }
}
