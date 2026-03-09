package preprocess.compress;


import utils.NumberUtil;

import java.util.Arrays;

/**
 * Simple8b压缩实现类
 * <p>
 * Simple-16 user words of 32bits (int). The first 4 bits of every 32-bit word is a bitsPerInteger that indicates an
 * encoding mode.
 * For example:
 * Selector values 0 or 1 represent sequences containing 112 and 56 zeros, respectively. In this
 * instance the 28 data bits are ignored.
 * The bitsPerInteger value 2 corresponds to b = 1. This allows us to store 28 integers having values in
 * f0,1g, which are packed in the data bits.
 * The bitsPerInteger value 3 corresponds to b = 2 and allows one to pack 14 integers having values in
 * [0,4] in the data bits.
 * </p>
 */
public class Simple8b implements IIntegerCompress {

    /**
     * index represents bits per integer
     * value represents max integers coded
     **/
    private final static int[] integers = new int[]{120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 6, 5, 4, 3, 2, 1};

    /**
     * bitsPerInteger
     */
    private final static int[] bitsPerInteger = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 20, 30, 60};

    /**
     * bitsPerInteger
     */

    @Override
    public byte[] encoding(int[] rawValues) {
        if (0 == rawValues.length) {
            return null;
        }
        int[] values = Arrays.copyOf(rawValues, rawValues.length);
        int valuesLength = values.length;
        long[] tmpResult = new long[valuesLength];
        int usedIntSize = 0;
        int low = 0;
        int maxBits = 0;
        int maxSelector;
        for (int i = 0; i < valuesLength; i++) {
            int currentBits = IIntegerCompress.getBitsLength(values[i]);
            int curSelector = findSelectorValue(currentBits);
            currentBits = bitsPerInteger[curSelector];
            maxSelector = findSelectorValue(maxBits);
            if (currentBits > maxBits) {
                while (i - low + 1 > integers[curSelector]) {
                    //根据现在的整数长度找到一个最合适的b来存储
                    //即 integers[x]  <= i -low <= integers[x+1],从b开始\
                    while (i - low < integers[maxSelector]) {
                        maxSelector++;
                    }
                    maxBits = bitsPerInteger[maxSelector];
                    for (int j = low; j < low + integers[maxSelector]; j++) {
                        tmpResult[usedIntSize] |= ((long) values[j] << (maxBits * (j - low)));
                    }

                    tmpResult[usedIntSize++] |= ((long) maxSelector << 60);
                    low += integers[maxSelector];
                }
                maxBits = currentBits;
                maxSelector = findSelectorValue(maxBits);
            }
            if (i - low + 1 == integers[maxSelector] || i == valuesLength - 1) {
                for (int j = low; j <= i; j++) {
                    tmpResult[usedIntSize] |= ((long) values[j] << (maxBits * (j - low)));
                }
                tmpResult[usedIntSize++] |= ((long) maxSelector << 60);
                low = i + 1;
                maxBits = 0;
            }
        }

        byte[] result = new byte[usedIntSize * 8 + 4];
        NumberUtil.copyLongToBytes(valuesLength, 4, result, 0);
        for (int i = 0; i < usedIntSize; i++) {
            NumberUtil.copyLongToBytes(tmpResult[i], 8, result, 4 + i * 8);
        }
        return result;
    }

    /**
     * 特殊的bits
     */
    private final int[] specialBits = new int[]{10, 12, 15, 20, 30, 60};

    /**
     * 获取selector的值
     *
     * @param currentBits 当前的bits数
     * @return : int selector值
     **/
    private int findSelectorValue(int currentBits) {
        int specialIndex = 0;
        if (currentBits <= specialBits[specialIndex++]) {
            return currentBits;
        }
        if (currentBits <= specialBits[specialIndex]) {
            return 11;
        } else {
            specialIndex++;
            if (currentBits <= specialBits[specialIndex]) {
                return 12;
            } else {
                specialIndex++;
                if (currentBits <= specialBits[specialIndex]) {
                    return 13;
                } else {
                    specialIndex++;
                    if (currentBits <= specialBits[specialIndex]) {
                        return 14;
                    } else {
                        return 15;
                    }
                }
            }
        }
    }

    @Override
    public int[] decoding(byte[] coding) {
        int valuesLength = NumberUtil.bytesToInt(coding, 4);
        int[] result = new int[valuesLength];
        int resultIndex = 0;
        for (int i = 4; i < coding.length; i += Long.SIZE / Byte.SIZE) {
            long value = NumberUtil.bytesToUnsignedLong(coding, i, i + 8);
            long selector = ((value >> 60) & 0x0f);
            int maxBits = bitsPerInteger[(int) selector];
            for (int j = 0; j < integers[(int) selector] && resultIndex < valuesLength; j++) {
                result[resultIndex++] = (int) (value & IIntegerCompress.nOf64One(maxBits));
                value >>= maxBits;
            }
        }
        return result;
    }
}
