package com.bw.kafka;

/**
 * 作者QQ：43281991
 */
public class KafkaDemo2 {
    public static void main(String[] args) {
        String str1 = "823183725,en_US,1975,male,2012-10-06T03:14:07.149Z,Stratford  Ontario,-240,,7,7,7,,,,";
        String str2 = "823183725,en_US,1975,male,2012-10-06T03:14:07.149Z,Stratford  Ontario,-240";
        String[] split = str1.split(",",-1);
        for (String str:split) {
            System.out.println(str);
        }
//        System.out.println(split.toString());
    }
}
