package utils;

/**
 * Java 包装类，用于字节数组操作
 * 提供与 Scala ByteArrays 对象相同的功能
 */
public class ByteArraysWrapper {
    
    /**
     * 写入 short 值到字节数组
     */
    public static void writeShort(short value, byte[] bytes, int offset) {
        bytes[offset] = (byte) (value >> 8);
        bytes[offset + 1] = (byte) value;
    }
    
    /**
     * 写入 int 值到字节数组
     */
    public static void writeInt(int value, byte[] bytes, int offset) {
        bytes[offset] = (byte) ((value >> 24) & 0xff);
        bytes[offset + 1] = (byte) ((value >> 16) & 0xff);
        bytes[offset + 2] = (byte) ((value >> 8) & 0xff);
        bytes[offset + 3] = (byte) (value & 0xff);
    }
    
    /**
     * 写入 long 值到字节数组
     */
    public static void writeLong(long value, byte[] bytes, int offset) {
        bytes[offset] = (byte) ((value >> 56) & 0xff);
        bytes[offset + 1] = (byte) ((value >> 48) & 0xff);
        bytes[offset + 2] = (byte) ((value >> 40) & 0xff);
        bytes[offset + 3] = (byte) ((value >> 32) & 0xff);
        bytes[offset + 4] = (byte) ((value >> 24) & 0xff);
        bytes[offset + 5] = (byte) ((value >> 16) & 0xff);
        bytes[offset + 6] = (byte) ((value >> 8) & 0xff);
        bytes[offset + 7] = (byte) (value & 0xff);
    }
    
    /**
     * 从字节数组读取 short 值
     */
    public static short readShort(byte[] bytes, int offset) {
        return (short) (((bytes[offset] & 0xff) << 8) | (bytes[offset + 1] & 0xff));
    }
    
    /**
     * 从字节数组读取 int 值
     */
    public static int readInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 24) |
               ((bytes[offset + 1] & 0xff) << 16) |
               ((bytes[offset + 2] & 0xff) << 8) |
               (bytes[offset + 3] & 0xff);
    }
    
    /**
     * 从字节数组读取 long 值
     */
    public static long readLong(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xffL) << 56) |
               ((bytes[offset + 1] & 0xffL) << 48) |
               ((bytes[offset + 2] & 0xffL) << 40) |
               ((bytes[offset + 3] & 0xffL) << 32) |
               ((bytes[offset + 4] & 0xffL) << 24) |
               ((bytes[offset + 5] & 0xffL) << 16) |
               ((bytes[offset + 6] & 0xffL) << 8) |
               (bytes[offset + 7] & 0xffL);
    }
}
