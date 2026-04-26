package experiments.standalone.store;

import com.esri.core.geometry.Envelope;
import config.TableConfig;
import constans.IndexEnum;
import loader.Loader;
import preprocess.compress.IIntegerCompress;

import java.io.IOException;


public class TMANStoring {
    public static void main(String[] args) throws IOException {
        String dataFilePath = args[0];
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
//        System.out.println(pIndex.getIndexName());
//        System.out.println(args[12]);
        String tableName = args[13];
        String redisHost = args[14];
        int isXZ = Integer.parseInt(args[15]);
        int tspEncoding = Integer.parseInt(args[16]);
        String resultPath = args[17];
        TableConfig tableConfig = new TableConfig(pIndex, resolution, alpha, beta, timeBin, timeBinNums, compressType, new Envelope(xmin, ymin, xmax, ymax), shards);
        tableConfig.setTableName(tableName);
        tableConfig.setRedisHost(redisHost);
        tableConfig.setIsXZ(isXZ);
        tableConfig.setTspEncoding(tspEncoding);
        try (Loader loader = new Loader(tableConfig, dataFilePath, resultPath)) {
            loader.store();
        }
    }
}
