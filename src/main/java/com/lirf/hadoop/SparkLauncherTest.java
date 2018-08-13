package com.lirf.hadoop;

import org.apache.spark.launcher.SparkLauncher;

import java.io.*;


/**
 * 功能描述
 * @Author lirf
 * @Date 2017/9/30 9:51
 */
public class SparkLauncherTest {
    public static void main(String[] args) {
        Process spark = null;
        try
        {
            spark = new SparkLauncher()
                    .setSparkHome("D:\\spark")
                    .setAppResource("sparktest.jar")
                    .setMainClass("com.lirf.hadoop.WordCount")
                    .setMaster("spark://192.168.0.197:7077")
                    .launch();
            InputStream stdInput = spark.getInputStream();
            InputStream errInput = spark.getErrorStream();

            spark.waitFor();

            // 获取dirver进程的输出
            System.out.println("---------------- read msg -----------------");
            dumpInput(stdInput);

            System.out.println("-------------- read err msg ---------------");
            toFile(errInput);
            spark.destroy();

            System.out.println("launcher over");
        }
        catch( IOException e)
        {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void dumpInput(InputStream input) throws IOException {
        byte[] buff = new byte[1024];
        while (true) {
            int len = input.read(buff);
            if (len < 0) {
                break;
            }
            System.out.println(new String(buff, 0, len));
        }
    }

    private static void toFile(InputStream input) throws IOException {
        FileOutputStream fos = new FileOutputStream("D:/err.txt");

        byte[] buffer = new byte[1024];

        while (input.read(buffer) != -1) {

            fos.write(buffer);
        }
    }
}
