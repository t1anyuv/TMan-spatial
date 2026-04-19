package client;

import com.esri.core.geometry.Envelope;

public class Constants {
    /**
     * 全局统一的空间边界配置
     * 格式: Envelope(xmin, ymin, xmax, ymax)
     * 值: (115.29, 39.00, 117.83, 41.50)
     */
    public static Envelope DEFAULT_ENVELOPE = new Envelope(115.29, 39.00, 117.83, 41.50);
    public static String META = "meta_";
    public static String PIVOT = "fa_pivot_";
    public static String DEFAULT_CF = "cf";
    public static String O_ID = "oid";

    public static String T_ID = "tid";
    public static String GEOM = "f_traj";
    public static String GEOM_X = "f_traj_X";
    public static String GEOM_Y = "f_traj_Y";
    public static String GEOM_Z = "f_traj_Z";
    public static String MAX_SFC = "max_sfc";
    public static String START_POINT = META + "start_point";
    public static String END_POINT = META + "end_point";
    public static String START_TIME = META + "start_time";
    public static String END_TIME = META + "end_time";

    public static String PIVOT_MBR = PIVOT + "mbr_point";
    public static String PIVOT_POINT = PIVOT + "index_point";
    public static Short DEFAULT_G = 16;

    public static String META_TABLE = "_meta";
    public static String META_TABLE_ROWKEY = "meta";
    public static String META_TABLE_SHARDS = "meta";
    public static String META_TABLE_PRIMARY = "primary_index";
    public static String META_TABLE_ALPHA = "alpha";
    public static String META_TABLE_RESOLUTION = "resolution";
    public static String META_TABLE_BETA = "beta";
    public static String META_TABLE_TIME_BIN = "time_bin";
    public static String META_TABLE_IS_XZ = "is_xz";
    public static String META_TABLE_IS_TSP_ENCODING = "is_tsp_encoding";
    public static String META_TABLE_ORDER_ENCODING_TYPE = "order_encoding_type";
    public static String META_TABLE_ORDER_DEFINITION_PATH = "order_definition_path";
    public static String META_TABLE_ADAPTIVE_PARTITION = "adaptive_partition";
    public static String META_TABLE_MAIN_TABLE_MOVE_BITS = "main_table_move_bits";
    public static String META_REDIS = "redis";
    public static String META_TABLE_TIME_BIN_NUMS = "time_bin_nums";
    public static String META_TABLE_TIME_PERIOD = "time_period";
    public static String META_TABLE_compression = "compression";
    public static String META_TABLE_xmin = "x_min";
    public static String META_TABLE_xmax = "x_max";
    public static String META_TABLE_ymin = "y_min";
    public static String META_TABLE_ymax = "y_max";
    
    public static final String META_TABLE_IS_LMSFC = "isLMSFC";
    public static final String META_TABLE_THETA_CONFIG = "thetaConfig";
    
    public static final String META_TABLE_IS_BMTREE = "isBMTree";
    public static final String META_TABLE_BMTREE_CONFIG_PATH = "bmtreeConfigPath";
    public static final String META_TABLE_BMTREE_BIT_LENGTH = "bmtreeBitLength";

    public static final String META_TABLE_LETI_ORDER_NAME = "letiOrderName";
    public static final String META_TABLE_LETI_ORDER_DATASET = "letiOrderDataset";
    public static final String META_TABLE_LETI_ORDER_DISTRIBUTION = "letiOrderDistribution";
    public static final String META_TABLE_LETI_ORDER_VERSION = "letiOrderVersion";
    public static final String META_TABLE_LETI_ORDER_COUNT = "letiOrderCount";
    public static final String META_TABLE_LETI_ACTIVE_CELLS = "letiActiveCells";
    public static final String META_TABLE_LETI_TOTAL_CELLS = "letiTotalCells";
    public static final String META_TABLE_LETI_MAX_LEVEL = "letiMaxLevel";
    public static final String META_TABLE_LETI_GLOBAL_ALPHA = "letiGlobalAlpha";
    public static final String META_TABLE_LETI_GLOBAL_BETA = "letiGlobalBeta";
    public static final String META_TABLE_LETI_BOUNDARY_XMIN = "letiBoundaryXMin";
    public static final String META_TABLE_LETI_BOUNDARY_XMAX = "letiBoundaryXMax";
    public static final String META_TABLE_LETI_BOUNDARY_YMIN = "letiBoundaryYMin";
    public static final String META_TABLE_LETI_BOUNDARY_YMAX = "letiBoundaryYMax";

    public static String STAT_TABLE = "_stat";
}
