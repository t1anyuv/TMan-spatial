package client;

import com.esri.core.geometry.Geometry;
import index.LocSIndex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static client.Constants.DEFAULT_CF;

public class HBaseClient {
    private short g;
    private int alpha;
    private int beta;
    private LocSIndex locSIndex;
    private Admin admin;
    private Table hTable;
    private String tableName;
    private Connection connection;

    public HBaseClient(String tableName, short g, int alpha, int beta) throws IOException {
        this(tableName, g, alpha, beta, LocSIndex.apply(g, alpha, beta));
    }

    public HBaseClient(String tableName, short g, int alpha, int beta, LocSIndex locSIndex) throws IOException {
        this.g = g;
        this.alpha = alpha;
        this.beta = beta;
        this.tableName = tableName;
        Configuration conf = HBaseConfiguration.create();
        this.connection = ConnectionFactory.createConnection(conf);
        this.admin = connection.getAdmin();
        this.tableName = tableName;
        this.locSIndex = locSIndex;
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        if (!admin.tableExists(table.getTableName())) {
            create();
        }
        this.hTable = this.connection.getTable(TableName.valueOf(tableName));
    }

    public void create() throws IOException {
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(this.tableName));
        if (this.admin.tableExists(table.getTableName())) {
            this.admin.disableTable(table.getTableName());
            this.admin.deleteTable(table.getTableName());
        }
        table.addFamily(new HColumnDescriptor(DEFAULT_CF));
        this.admin.createTable(table);
    }

//    public List<Geometry> rangeQuery(double lat1, double lng1, double lat2, double lng2, Map<Integer, List<Integer>> indexMap) {
//        //locSIndex.ranges(lat1, lng1, lat2, lng2, indexMap)
//    }
}
