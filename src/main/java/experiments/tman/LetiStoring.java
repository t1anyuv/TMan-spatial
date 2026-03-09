package experiments.tman;

import com.esri.core.geometry.Envelope;
import config.TableConfig;
import constans.IndexEnum;
import loader.LetiLoader;
import preprocess.compress.IIntegerCompress;

import java.io.IOException;

public class LetiStoring {
    public static void main(String[] args) throws IOException {
        String sourcePath = args[0];
        int resolution = Integer.parseInt(args[1]);
        int alpha = Integer.parseInt(args[2]);
        int beta = Integer.parseInt(args[3]);
        double timeBin = Double.parseDouble(args[4]);
        int timeBinNums = Integer.parseInt(args[5]);
        IIntegerCompress.CompressType compressType = IIntegerCompress.CompressType.valueOf(args[6]);
        double xmin = Double.parseDouble(args[7]);
        double ymin = Double.parseDouble(args[8]);
        double xmax = Double.parseDouble(args[9]);
        double ymax = Double.parseDouble(args[10]);
        short shards = Short.parseShort(args[11]);
        IndexEnum.INDEX_TYPE pIndex = IndexEnum.INDEX_TYPE.valueOf(args[12]);
        System.out.println(pIndex.getIndexName());
        System.out.println(args[12]);
        String tableName = args[13];
        String redisHost = args[14];
        int adaptivePartition = Integer.parseInt(args[15]);
        String resultPath = args[16];

        TableConfig tableConfig = new TableConfig(
                pIndex, resolution, alpha, beta, timeBin, timeBinNums, compressType,
                new Envelope(xmin, ymin, xmax, ymax), shards
        );
        tableConfig.setTableName(tableName);
        tableConfig.setRedisHost(redisHost);
        tableConfig.setAdaptivePartition(adaptivePartition);

        try (LetiLoader loader = new LetiLoader(tableConfig, sourcePath, resultPath)) {
            loader.store();
        }
    }
}





