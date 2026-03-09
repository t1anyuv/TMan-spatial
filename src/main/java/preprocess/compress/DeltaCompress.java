package preprocess.compress;

import utils.NumberUtil;

import java.util.Arrays;

/**
 * Delta压缩实现类
 */
public class DeltaCompress implements IIntegerCompress {
    /**
     * 是否是排好序的数组
     */
    private boolean isSorted;

    /**
     * 整数压缩方法
     */
    private final IIntegerCompress integerCompress;

    /**
     * DeltaCompress的构造函数
     */
    public DeltaCompress(boolean isSorted, IIntegerCompress integerCompress) {
        this.isSorted = isSorted;
        this.integerCompress = integerCompress;
    }

    /**
     * DeltaCompress的构造函数
     */
    public DeltaCompress(IIntegerCompress integerCompress) {
        this(false, integerCompress);
    }

    /**
     * DeltaCompress的构造函数
     */
    public DeltaCompress(boolean isSorted) {
        this(isSorted, new VarintGB());
    }

    /**
     * DeltaCompress的构造函数
     */
    public DeltaCompress() {
        this(false, new VarintGB());
    }

    public void setSorted(boolean sorted) {
        isSorted = sorted;
    }

    @Override
    public byte[] encoding(int[] rawValues) {
        if (0 == rawValues.length) {
            return null;
        }
        int[] values = Arrays.copyOf(rawValues, rawValues.length);
        byte[] result;
        if (isSorted) {
            for (int i = values.length - 1; i > 0; i--) {
                values[i] -= values[i - 1];
            }
            return integerCompress.encoding(values);
        } else {
            int minimumValue = Integer.MAX_VALUE;
            for (int value : values) {
                if (minimumValue > value) {
                    minimumValue = value;
                }
            }
            for (int i = 0; i < values.length; i++) {
                values[i] -= minimumValue;
            }
            result = integerCompress.encoding(values);
            byte[] finalResult = new byte[result.length + 4];
            NumberUtil.copyLongToBytes(minimumValue, 4, finalResult, 0);
            System.arraycopy(result, 0, finalResult, 4, result.length);
            return finalResult;
        }
    }

    @Override
    public int[] decoding(byte[] coding) {
        if (isSorted) {
            int[] values = integerCompress.decoding(coding);
            for (int i = 1; i < values.length; i++) {
                values[i] += values[i - 1];
            }
            return values;
        } else {
            int[] values = integerCompress.decoding(Arrays.copyOfRange(coding, 4, coding.length));
            int minimumValue = NumberUtil.bytesToUnsignedInt(coding, 4);
            for (int i = 0; i < values.length; i++) {
                values[i] += minimumValue;
            }
            return values;
        }
    }
}
