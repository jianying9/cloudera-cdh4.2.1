package com.wolf.mapred;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author aladdin
 */
public class PartitionUtils {

//    public final static int LENGTH = 65535;
    public final static int REGION_NUM = 512;

    /**
     * 根据imei的hashCode值，散列到65536分区,返回分区的int值
     *
     * @param imei
     * @return
     */
//    public static int getPartition(String imei) {
//        int result;//return
//        int h = 0;
//        int hash = imei.hashCode();
//        h ^= hash;
//        h ^= (h >>> 20) ^ (h >>> 12);
//        h = h ^ (h >>> 7) ^ (h >>> 4);
//        result = h & LENGTH;
//        return result;
//    }
    public static String getPartition(String imei) {
        String result = PartitionUtils.encryptByMd5(imei);
        if(result.isEmpty()) {
            result = "0000";
        } else {
            result = result.substring(0, 4);
            result = result.toLowerCase();
        }
        return result;
    }

    /**
     * 获取hTbale的region的startKey集合
     *
     * @return
     */
    public static List<String> getPartitionStartKeyList() {
        List<String> result = new ArrayList<String>(REGION_NUM);
        long start = 0;
        long d = 8388608;
        String region;
        for (int index = 0; index < 512; index++) {
            region = String.format("%08x", start);
            result.add(region);
            start += d;
        }
        return result;
    }

    /**
     *
     * @param str
     * @return
     */
    public static String encryptByMd5(String str) {
        String result = "";
        try {
            MessageDigest algorithm = MessageDigest.getInstance("MD5");
            algorithm.reset();
            byte[] messageDigest = algorithm.digest(str.getBytes());
            result = byteToHexString(messageDigest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    private static String byteToHexString(byte[] textByte) {
        StringBuilder hexString = new StringBuilder(32);
        int byteValue;
        for (byte bt : textByte) {
            byteValue = 0xFF & bt;
            if (byteValue < 16) {
                hexString.append("0").append(Integer.toHexString(byteValue));
            } else {
                hexString.append(Integer.toHexString(byteValue));
            }
        }
        return hexString.toString();
    }
}
