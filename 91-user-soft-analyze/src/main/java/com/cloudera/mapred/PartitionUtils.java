package com.cloudera.mapred;

/**
 *
 * @author aladdin
 */
public class PartitionUtils {

    /**
     * 根据imei的hashCode值，散列到65536分区,返回分区的16进制字符串
     *
     * @param imei
     * @return
     */
    public static int getPartition(String imei) {
        int result;//return
        final int length = 65535;
        int h = 0;
        int hash = imei.hashCode();
        h ^= hash;
        h ^= (h >>> 20) ^ (h >>> 12);
        h = h ^ (h >>> 7) ^ (h >>> 4);
        result = h & length;
//        result = String.format("%04x", part);
        return result;
    }
}
