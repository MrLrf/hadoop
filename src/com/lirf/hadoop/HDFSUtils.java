package com.lirf.hadoop;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by liuzg on 2017/6/28.
 * HDFS工具类用于操作HDFS文件系统
 */
public class HDFSUtils {

    static {
        System.setProperty("HADOOP_USER_NAME","root");
    }

    public static boolean mkdir(String uri, Configuration conf, String dir){
        try {
            FileSystem fs = FileSystem.get(URI.create(uri),conf);
            fs.mkdirs(new Path(dir));
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
    public static boolean uploadFile(String uri, Configuration conf, String localfilepath, String destpath){
        try {
            FileSystem fs = FileSystem.get(URI.create(uri),conf);
            fs.copyFromLocalFile(new Path(localfilepath),new Path(destpath));
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static boolean downloadFile(String uri, Configuration conf, String remotefspath, String localpath){
        try {
            FileSystem fs = FileSystem.get(URI.create(uri),conf);
            fs.copyToLocalFile(new Path(remotefspath),new Path(localpath));
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static InputStream downloadFileStream(String uri, Configuration conf, String remotefspath){
        try {
            FileSystem fs = FileSystem.get(URI.create(uri),conf);
            FSDataInputStream stream = fs.open(new Path(remotefspath));
            return stream;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static boolean uploadFileStream(String uri, Configuration conf, InputStream stream, String remotefspath){
        try {
            FileSystem fs = FileSystem.get(URI.create(uri),conf);
            FSDataOutputStream hdoutstream = fs.create(new Path(remotefspath), true);
            IOUtils.copy(stream,hdoutstream);
            hdoutstream.flush();
            hdoutstream.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static FileStatus getFileStatus(String uri, Configuration conf, String path){
        try {
            FileSystem fs = FileSystem.get(URI.create(uri),conf);
            return fs.getFileStatus(new Path(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static boolean deleteFile(String uri, Configuration conf, String dir){
        try {
            FileSystem fs = FileSystem.get(URI.create(uri),conf);
            fs.delete(new Path(dir),true);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;

    }

    public static List<LocatedFileStatus> listFiles(String uri, Configuration conf, String dir, boolean recursive){
        try {
            FileSystem fs = FileSystem.get(URI.create(uri),conf);
            RemoteIterator<LocatedFileStatus> filesit = fs.listFiles(new Path(dir), recursive);
            List<LocatedFileStatus> files = new ArrayList<>();
            while (filesit.hasNext()){
                LocatedFileStatus file = filesit.next();
                files.add(file);
            }
            return files;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
