package experiments.suite.similarity.output;

import experiments.suite.similarity.model.SimilarityExperimentResult;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class SimilaritySummaryWriter {
    private SimilaritySummaryWriter() {
    }

    public static void write(Path outputFile, List<SimilarityExperimentResult> results) throws IOException {
        if (outputFile.getParent() != null) {
            Files.createDirectories(outputFile.getParent());
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile.toFile()))) {
            writer.write("dataset,query_type,parameter_name,parameter_value,distance_function,method,avg_latency_ms,avg_row_key_ranges,avg_candidates,avg_final_size,avg_iterations,query_count");
            writer.newLine();
            for (SimilarityExperimentResult result : results) {
                writer.write(result.toCsvRow());
                writer.newLine();
            }
        }
    }
}
