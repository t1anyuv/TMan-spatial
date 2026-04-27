package experiments.common.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public final class ExperimentPaths {
    private ExperimentPaths() {
    }

    public static List<String> readAllLines(String inputPath) throws IOException {
        return readAllLines(inputPath, StandardCharsets.UTF_8);
    }

    public static List<String> readAllLines(String inputPath, Charset charset) throws IOException {
        String normalized = normalize(inputPath);
        if (!isDistributedPath(normalized)) {
            return Files.readAllLines(Paths.get(normalized), charset);
        }

        Configuration conf = new Configuration();
        Path path = new Path(normalized);
        List<String> lines = new ArrayList<>();
        try (FileSystem fs = path.getFileSystem(conf);
             BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path), charset))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }
        return lines;
    }

    public static byte[] readAllBytes(String inputPath) throws IOException {
        String normalized = normalize(inputPath);
        if (!isDistributedPath(normalized)) {
            return Files.readAllBytes(Paths.get(normalized));
        }

        Configuration conf = new Configuration();
        Path path = new Path(normalized);
        try (FileSystem fs = path.getFileSystem(conf);
             org.apache.hadoop.fs.FSDataInputStream input = fs.open(path)) {
            long length = fs.getFileStatus(path).getLen();
            if (length > Integer.MAX_VALUE) {
                throw new IOException("Path too large to read into memory: " + inputPath);
            }
            byte[] data = new byte[(int) length];
            input.readFully(0L, data);
            return data;
        }
    }

    public static String readUtf8String(String inputPath) throws IOException {
        return new String(readAllBytes(inputPath), StandardCharsets.UTF_8);
    }

    public static boolean isDistributedPath(String inputPath) {
        String normalized = inputPath == null ? "" : inputPath.trim().toLowerCase(Locale.ROOT);
        return normalized.startsWith("hdfs://")
                || normalized.startsWith("viewfs://")
                || normalized.startsWith("webhdfs://");
    }

    public static String normalize(String inputPath) {
        if (inputPath == null || inputPath.trim().isEmpty()) {
            throw new IllegalArgumentException("Input path must not be empty");
        }
        return inputPath.trim();
    }
}
