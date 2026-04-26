package experiments.suite.parameter.output;

import experiments.suite.parameter.model.ParameterBuildRow;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class ParameterBuildSummaryWriter {
    private ParameterBuildSummaryWriter() {
    }

    public static void write(Path outputFile, List<ParameterBuildRow> rows) throws IOException {
        if (outputFile.getParent() != null) {
            Files.createDirectories(outputFile.getParent());
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile.toFile()))) {
            writer.write("dataset,table_name,study,resolution,min_traj,trajectory_count,node_count,shape_count,build_time_ms,main_table_bytes,index_table_bytes,extra_index_info_bytes,total_bytes,note");
            writer.newLine();
            for (ParameterBuildRow row : rows) {
                writer.write(row.toCsvRow());
                writer.newLine();
            }
        }
    }
}
