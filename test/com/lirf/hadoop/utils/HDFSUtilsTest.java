package com.lirf.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.junit.Test;

import java.io.*;
import java.util.List;

/**
 * 功能描述
 * @Author lirf
 * @Date 2017/10/18 20:07
 */
public class HDFSUtilsTest {
    private static final String URI = "hdfs://192.168.0.196";

    @Test
    public void test1() {
        String content = "1234567";
        InputStream is = null;
        try {
            is = new ByteArrayInputStream(content.getBytes("UTF-8"));
            String path = "/2";
            boolean r = HDFSUtils.uploadFileStream(URI, new Configuration(), is, path);
            is.close();
            System.out.println(r);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test2() {
        String path = "/.IntelliJIdea2017.1.zip";
        String localPath = "D:\\lirf\\备份\\.IntelliJIdea2017.1.zip";
        boolean r = HDFSUtils.uploadFile(URI, new Configuration(), localPath, path);
        System.out.println(r);
    }

    @Test
    public void test3() {
        String path = "/.IntelliJIdea2017.1.zip";
        FileStatus fs = HDFSUtils.getFileStatus(URI, new Configuration(), path);
        System.out.println(fs);
    }

    @Test
    public void test4() {
        List<LocatedFileStatus> locatedFileStatuses = HDFSUtils.listFiles(URI, new Configuration(), "/", false);
        for (LocatedFileStatus locatedFileStatus : locatedFileStatuses) {
            System.out.println(locatedFileStatus);
        }
    }

    @Test
    public void test5() {
        List<DatanodeInfo> dataNodeInfos = HDFSUtils.getHostNames(URI, new Configuration());
        for (DatanodeInfo datanodeInfo : dataNodeInfos) {
            System.out.println(datanodeInfo);
        }
    }

}
