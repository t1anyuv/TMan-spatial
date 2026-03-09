package preprocess.compress;


import utils.NumberUtil;

import java.util.Arrays;

/**
 * Delta压缩实现类2
 */
public class DeltaCompress2 implements IIntegerCompress {
    /**
     * 最大字节数
     **/
    private int perByteSize;

    /**
     * 第二整数压缩(必须是支持负数的压缩)
     */
    private IIntegerCompress integerCompress;

    /**
     * 增量编码
     *
     **/
    public DeltaCompress2(IIntegerCompress integerCompress) {
        this.integerCompress = integerCompress;
    }

    /**
     * DeltaCompress2的构造函数
     */
    public DeltaCompress2(int perByteSize) {
        this.perByteSize = perByteSize;
    }

    @Override
    public byte[] encoding(int[] rawValues) {
        if (0 == rawValues.length) {
            return null;
        }
        int[] values = Arrays.copyOf(rawValues, rawValues.length);
        byte[] result = new byte[values.length * 5];
        for (int i = values.length - 1; i > 0; i--) {
            values[i] -= values[i - 1];
        }
        if (null != this.integerCompress) {
            return integerCompress.encoding(values);
        }
        int index = 0;
        for (int value : values) {
            int bytesSize = IIntegerCompress.getBytesLength(value);
            if (bytesSize > this.perByteSize) {
                result[index++] = -1;
                NumberUtil.copyLongToBytes(value, 4, result, index);
                index += 4;
            } else {
                NumberUtil.copyLongToBytes(value, this.perByteSize, result, index);
                index += this.perByteSize;
            }
        }
        return Arrays.copyOfRange(result, 0, index);

    }

    @Override
    public int[] decoding(byte[] coding) {
        if (null != integerCompress) {
            int[] values = integerCompress.decoding(coding);
            for (int i = 1; i < values.length; i++) {
                values[i] += values[i - 1];
            }
            return values;
        }
        int[] result = new int[coding.length];
        int resultIndex = 0;
        int index = 0;
        while (index < coding.length) {
            if (coding[index] == -1) {
                result[resultIndex] = NumberUtil.bytesToInt(coding, index + 1, index + 5);
                index += 5;
            } else {
                result[resultIndex] = NumberUtil.bytesToInt(coding, index, index + this.perByteSize);
                index += this.perByteSize;
            }
            if (resultIndex != 0) {
                result[resultIndex] += result[resultIndex - 1];
            }
            resultIndex++;
        }

        return Arrays.copyOfRange(result, 0, resultIndex);
    }
}
