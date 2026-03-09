package preprocess.compress;

import java.util.NoSuchElementException;


public interface IIntegerCompress {

    enum CompressType {
        /**
         * VGB编码
         */
        VGB,
        /**
         * DELTA_COMPRESS_VGB编码
         */
        DELTA_COMPRESS_VGB,
        /**
         * DELTA_COMPRESS_SIMPLE8B编码
         */
        DELTA_COMPRESS_SIMPLE8B,
        /**
         * DELTA_COMPRESS_NEW_PFD_SIMPLE8B编码
         */
        DELTA_COMPRESS_NEW_PFD_SIMPLE8B,
        /**
         * DELTA_COMPRESS_NEW_PFD_SIMPLE8B_SORTED编码
         */
        DELTA_COMPRESS_NEW_PFD_SIMPLE8B_SORTED,
        /**
         * DELTA_COMPRESS_OPT_PFD_SIMPLE8B编码
         */
        DELTA_COMPRESS_OPT_PFD_SIMPLE8B,
        /**
         * DELTA_COMPRESS2编码
         */
        DELTA_COMPRESS2,

        /**
         * PFOR_VGB, 20bits编码
         */
        PFOR_VGB_20,
        /**
         * PFOR_SIMPLE8B, 20bits编码
         */
        PFOR_SIMPLE8B_20,
        /**
         * NEW_PFD_VGB编码
         */
        NEW_PFD_VGB,
        /**
         * OPT_PFD_VGB编码
         */
        OPT_PFD_VGB,
        /**
         * NEW_PFD_SIMPLE8B编码
         */
        NEW_PFD_SIMPLE8B,
        /**
         * OPT_PFD_SIMPLE8B编码
         */
        OPT_PFD_SIMPLE8B
    }

    /**
     * 编码
     *
     * @param rawValues 整数数组
     * @return : byte[]
     **/

    byte[] encoding(int[] rawValues);

    /**
     * 解码
     *
     * @param coding 编码字节数组
     * @return : int[]
     **/
    int[] decoding(byte[] coding);

    /**
     * 计算整数的字节数
     *
     * @param value 整数
     * @return : int
     **/
    static int getBytesLength(long value) {
        if (value < 0) {
            return (int) Math.ceil((Math.log(Math.abs(value)) / Math.log(2) + 1) / 8);
        }
        return (int) (Math.ceil((Math.log(value + 1) / Math.log(2) + 1) / 8.0));
    }

    /**
     * 表示一个整数所需的bits
     *
     * @param value 一个整数
     * @return : int
     **/
    static int getBitsLength(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("value = " + value + ";参数不能为负数!");
        }
        if (0 == value) {
            return 1;
        }
        return (int) Math.ceil((Math.log(value + 1) / Math.log(2)));
    }

    /**
     * bits的字节大小
     *
     * @param bits bits
     * @return : int
     **/
    static int bytesLengthOfBits(int bits) {
        return (int) Math.ceil(bits / 8.0);
    }

    /**
     * 生成n个连续1的二进制数
     *
     * @param n 个数
     * @return : int n个连续1的二进制表示的整数
     **/
    static int nOfOne(int n) {
        int result = 0;
        for (int i = 0; i < n; i++) {
            result |= 1 << i;
        }
        return result;
    }

    /**
     * 生成n个连续1的二进制数
     *
     * @param n 个数
     * @return : int n个连续1的二进制表示的整数
     **/
    static long nOf64One(int n) {
        long result = 0;
        for (int i = 0; i < n; i++) {
            result |= 1L << i;
        }
        return result;
    }

    /**
     * 获取压缩方法
     *
     * @param compressName 压缩方法名称
     * @return : com.urbancomputing.geomesa.algorithm.compress.IIntegerCompress
     **/
    static IIntegerCompress getIntegerCompress(String compressName) {
        switch (CompressType.valueOf(compressName)) {
            case VGB:
                return new VarintGB();
            case NEW_PFD_VGB:
                return new PFORCompress(PFORCompress.PFOR_TYPE.NEW_PFD);
            case OPT_PFD_VGB:
                return new PFORCompress(PFORCompress.PFOR_TYPE.OPT_PFD);
            case DELTA_COMPRESS2:
                return new DeltaCompress2(new VarintGB());
            case PFOR_SIMPLE8B_20:
                return new PFORCompress(20, new Simple8b());
            case PFOR_VGB_20:
                return new PFORCompress(20, new VarintGB());
            case NEW_PFD_SIMPLE8B:
                return new PFORCompress(PFORCompress.PFOR_TYPE.NEW_PFD, new Simple8b());
            case OPT_PFD_SIMPLE8B:
                return new PFORCompress(PFORCompress.PFOR_TYPE.OPT_PFD, new Simple8b());
            case DELTA_COMPRESS_VGB:
                return new DeltaCompress(false);
            case DELTA_COMPRESS_SIMPLE8B:
                return new DeltaCompress(false, new Simple8b());
            case DELTA_COMPRESS_NEW_PFD_SIMPLE8B:
                return new DeltaCompress(false, new PFORCompress(PFORCompress.PFOR_TYPE.NEW_PFD, new Simple8b()));
            case DELTA_COMPRESS_NEW_PFD_SIMPLE8B_SORTED:
                return new DeltaCompress(true, new PFORCompress(PFORCompress.PFOR_TYPE.NEW_PFD, new Simple8b()));
            case DELTA_COMPRESS_OPT_PFD_SIMPLE8B:
                return new DeltaCompress(false, new PFORCompress(PFORCompress.PFOR_TYPE.OPT_PFD, new Simple8b()));
            default:
                throw new NoSuchElementException("没有这种压缩方法");
        }
    }
}
