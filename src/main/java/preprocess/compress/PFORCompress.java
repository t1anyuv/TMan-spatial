package preprocess.compress;


import utils.NumberUtil;

import java.util.Arrays;

/**
 * PFOR压缩实现类
 * <p>
 * 首先得到表示每一个整数的bit数目，然后确定一个bits数b来存储每一个整数。如果某一位整数的bits数
 * 超过了b，那么该值位异常值的高bit - b位需要单独存储，而原位置处存储低b位，如下例。
 * 最后将得到的异常值和位置利用其他整数编码方法进行编码
 * </p>
 */
public class PFORCompress implements IIntegerCompress {

    /**
     * 最大bits
     */
    private int b = 1;
    /**
     * 整数压缩
     */
    private final IIntegerCompress integerCompress;

    /**
     * 编码类型
     */
    private PFOR_TYPE type = null;

    public enum PFOR_TYPE {
        /**
         * NEW_PFD
         */
        NEW_PFD,
        /**
         * OPT_PFD
         */
        OPT_PFD
    }

    /**
     * PFORCompress的构造函数
     */
    public PFORCompress(PFOR_TYPE type) {
        this(type, new VarintGB());
    }

    /**
     * PFORCompress的构造函数
     */
    public PFORCompress(PFOR_TYPE type, IIntegerCompress integerCompress) {
        this.type = type;
        this.integerCompress = integerCompress;
    }

    /**
     * BitsCompress的构造函数
     */
    public PFORCompress(int b) {
        this(b, new VarintGB());
    }

    /**
     * BitsCompress的构造函数
     *
     **/
    public PFORCompress(int b, IIntegerCompress integerCompress) {
        if (b <= 0) {
            throw new IllegalArgumentException("b must greater than 0");
        }
        this.b = b;
        this.integerCompress = integerCompress;
    }

    /**
     * BitsCompress的构造函数
     */
    public PFORCompress(IIntegerCompress integerCompress) {
        this.integerCompress = integerCompress;
    }

    @Override
    public byte[] encoding(int[] rawValues) {
        if (0 == rawValues.length) {
            return null;
        }
        int[] values = Arrays.copyOf(rawValues, rawValues.length);
        int valuesLength = values.length;
        int[] bits = new int[values.length];

        int[] best = findBestBits(bits, values);
        int bestBits = best[0];
        int exceptionSize = best[1];
        //使用最佳bits存储
        int size = (int) (5 + exceptionSize * 2 * 5 + Math.ceil((valuesLength * bestBits) / 8.0));
        byte[] result = new byte[size];
        //bestBits最大位32，所以用一个byte便可以表示
        result[0] = (byte) bestBits;
        int[] exceptionValues = new int[exceptionSize];
        int[] positions = new int[exceptionSize];
        int exceptionIndex = 0;
        // 记录位置,和剩余的bits
        // result第0个byte记录b，1-4个byte记录currentIndex的真实值
        int currentIndex = 5;
        //当前字节剩余未赋值位数
        int retainBitsOfCurByte = Byte.SIZE;
        //当前值剩余未复制位数
        int retainBitsOfCurValue;
        for (int i = 0; i < valuesLength; i++) {
            retainBitsOfCurValue = bestBits;
            while (retainBitsOfCurValue > 0) {
                if (retainBitsOfCurByte <= retainBitsOfCurValue) {
                    int tmpValue = (values[i] & IIntegerCompress.nOfOne(retainBitsOfCurByte));
                    values[i] >>= retainBitsOfCurByte;
                    //为当前byte的最后几位bits赋值
                    result[currentIndex] |= (byte) (tmpValue << (Byte.SIZE - retainBitsOfCurByte));
                    retainBitsOfCurValue -= retainBitsOfCurByte;
                    retainBitsOfCurByte = Byte.SIZE;
                    currentIndex++;
                } else {
                    int tmpValue = (values[i] & IIntegerCompress.nOfOne(retainBitsOfCurValue));
                    values[i] >>= retainBitsOfCurValue;
                    result[currentIndex] |= (byte) (tmpValue << (Byte.SIZE - retainBitsOfCurByte));
                    retainBitsOfCurByte -= retainBitsOfCurValue;
                    retainBitsOfCurValue = 0;
                }
            }
            //如果是异常值，则将其剩下的bits放入异常数组中
            if (bits[i] > bestBits) {
                positions[exceptionIndex] = i;
                exceptionValues[exceptionIndex] = (values[i] & IIntegerCompress.nOfOne(bits[i] - bestBits));
                exceptionIndex++;
            }
        }

        //存储额外的数据: bestBits,valuesLength,exceptionSize,positions,exceptionValues
        int extendDataSize = exceptionSize * 2;
        int[] additionalData = new int[extendDataSize];
        int additionalByteSize = 0;
        if (retainBitsOfCurByte != Byte.SIZE) {
            currentIndex++;
        }
        if (exceptionSize > 0) {
            System.arraycopy(positions, 0, additionalData, 0, positions.length);
            System.arraycopy(exceptionValues, 0, additionalData, positions.length, exceptionValues.length);
            byte[] additionalBytes = integerCompress.encoding(additionalData);
            additionalByteSize = additionalBytes.length;
            System.arraycopy(additionalBytes, 0, result, currentIndex, additionalBytes.length);
        }

        result[0] = (byte) bestBits;

        NumberUtil.copyLongToBytes(currentIndex, 4, result, 1);
        return Arrays.copyOfRange(result, 0, currentIndex + additionalByteSize);
    }


    @Override
    public int[] decoding(byte[] coding) {
        if (null == coding || coding.length == 0) {
            return null;
        }
        int b = coding[0];
        int firstPartEndIndex = NumberUtil.bytesToUnsignedInt(coding, 1, 5);
        byte[] firstPart = Arrays.copyOfRange(coding, 5, firstPartEndIndex);
        int curByteRetainBits;
        int curValueReqBits = b;
        int[] result = new int[firstPartEndIndex * Byte.SIZE / b];
        int resultIndex = 0;
        for (int i = 0; i < firstPart.length; i++) {
            curByteRetainBits = Byte.SIZE;
            while (curByteRetainBits > 0) {
                if (curValueReqBits <= curByteRetainBits) {
                    result[resultIndex++] |= ((firstPart[i] & IIntegerCompress.nOfOne(curValueReqBits)) << (b - curValueReqBits));
                    firstPart[i] >>= curValueReqBits;
                    curByteRetainBits -= curValueReqBits;
                    curValueReqBits = b;
                } else {
                    result[resultIndex] |= ((firstPart[i] & IIntegerCompress.nOfOne(curByteRetainBits)) << (b - curValueReqBits));
                    curValueReqBits -= curByteRetainBits;
                    break;
                }
            }
        }
        byte[] secondPart = Arrays.copyOfRange(coding, firstPartEndIndex, coding.length);
        if (0 != secondPart.length) {
            int[] secondValue = integerCompress.decoding(secondPart);
            int exceptionSize = secondValue.length / 2;

            for (int i = 0; i < exceptionSize; i++) {
                int position = secondValue[i];
                result[position] |= (secondValue[i + exceptionSize] << b);
            }
        }

        return Arrays.copyOfRange(result, 0, resultIndex);
    }

    /**
     * 计算最佳存储使用的bits数
     *
     * @param bits   整数bits数组
     * @param values 值
     * @return : int[]
     **/
    private int[] findBestBits(final int[] bits, int[] values) {
        int valuesLength = values.length;
        int minBit = Integer.MAX_VALUE;
        int maxBit = Integer.MIN_VALUE;
        for (int i = 0; i < valuesLength; i++) {
            bits[i] = IIntegerCompress.getBitsLength(values[i]);
            if (minBit > bits[i]) {
                minBit = bits[i];
            }
            if (maxBit < bits[i]) {
                maxBit = bits[i];
            }
        }


        if (null != this.type) {
            switch (this.type) {
                case NEW_PFD:
                    return newPFDBestBits(bits);
                case OPT_PFD:
                    return optPFDBestBits(bits, maxBit, minBit);
                default:
                    break;
            }
        }
        //指定b
        int exceptionSize = 0;
        for (int i = 0; i < valuesLength; i++) {
            if (bits[i] > this.b) {
                exceptionSize++;
            }
        }
        return new int[]{this.b, exceptionSize};
    }

    /**
     * optPFD的最佳bits
     *
     * @return : int[]
     **/
    private int[] optPFDBestBits(int[] bits, int maxBit, int minBit) {
        int valuesLength = bits.length;
        int bestBits = maxBit;
        int exceptionSize = 0;
        int minimumStoreBytes = IIntegerCompress.bytesLengthOfBits(valuesLength * maxBit);
        for (int i = maxBit - 1; i >= minBit; i--) {
            exceptionSize = 0;
            int storeSize = IIntegerCompress.bytesLengthOfBits(valuesLength * i);
            for (int j = 0; j < valuesLength && storeSize < minimumStoreBytes; j++) {
                if (bits[j] > i) {
                    storeSize += IIntegerCompress.bytesLengthOfBits(bits[j]);
                    exceptionSize++;
                }
            }
            storeSize += (int) Math.ceil(exceptionSize / 4.0);
            if (storeSize < minimumStoreBytes) {
                minimumStoreBytes = storeSize;
            }
        }
        return new int[]{bestBits, exceptionSize};
    }

    /**
     * newPFD的最佳bits
     *
     * @return : int[]
     **/
    private int[] newPFDBestBits(int[] bits) {
        int[] bitsNumber = new int[Integer.SIZE + 1];
        Arrays.fill(bitsNumber, 0);
        int maxBits = 0;
        for (int i = 0; i < bits.length; i++) {
            bitsNumber[bits[i]]++;
            if (maxBits < bits[i]) {
                maxBits = bits[i];
            }
        }
        int sum = 0;
        for (int i = 0; i < bitsNumber.length; i++) {
            sum += bitsNumber[i];
            if (sum / (float) bits.length >= 0.9) {
                return new int[]{i, bits.length - sum};
            }
        }
        return new int[]{maxBits, 0};
    }
}
