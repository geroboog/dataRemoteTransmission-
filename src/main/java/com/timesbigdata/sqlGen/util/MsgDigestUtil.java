package com.timesbigdata.sqlGen.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by Link on 2017/5/10.
 */
public class MsgDigestUtil {

    /**
     * 32位MD5
     * @param msg 要摘要的字节数组
     * @return MD5 Str
     */
    public static String getMD5(byte[] msg) {
        String result = "";
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(msg);
            byte b[] = md.digest();
            int i;
            StringBuilder buf = new StringBuilder("");
            for (byte aB : b) {
                i = aB;
                if (i < 0)
                    i += 256;
                if (i < 16)
                    buf.append("0");
                buf.append(Integer.toHexString(i));
            }
            result = buf.toString();
//            System.out.println("MD5(" + msg + ",32) = " + result);
//            System.out.println("MD5(" + msg + ",16) = " + buf.toString().substring(8, 24));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 32位MD5
     * @param msg 要摘要的字符串
     * @return MD5 Str
     */
    public static String getMD5(String msg) {
        return getMD5(msg.getBytes());
    }

    /**
     * 40位SHA1
     * @param msg 要摘要的字符串
     * @return SHA1
     */
    public static String getSHA1(String msg) {
        return getSHA1(msg.getBytes());
    }

    /**
     * 40位SHA1
     * @param msg 要摘要的字节数组
     * @return SHA1
     */
    public static String getSHA1(byte[] msg) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-1");
            digest.update(msg);
            byte messageDigest[] = digest.digest();
            // Create Hex String
            StringBuilder hexString = new StringBuilder();
            // 字节数组转换为 十六进制 数
            for (byte aMessageDigest : messageDigest) {
                String shaHex = Integer.toHexString(aMessageDigest & 0xFF);
                if (shaHex.length() < 2) {
                    hexString.append(0);
                }
                hexString.append(shaHex);
            }
            return hexString.toString();

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return "";
    }
}
