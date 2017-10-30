package com.lirf.hadoop.utils;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.*;

/**
 * HDFS工具类 用于操作HDFS文件系统
 * @Author lirf
 * @Date 2017/10/28 14:06
 */
public class HDFSUtils {
    /**
     * 创建目录
     * @param uri
     * @param conf
     * @param dir
     * @return
     */
    public static boolean mkdir(String uri, Configuration conf, String dir) {
        try {
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            fs.mkdirs(new Path(dir));
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 将输入流上传到文件
     * @param uri
     * @param conf
     * @param stream 内容输入流
     * @param remoteFSPath HDFS保存内容的文件路径
     * @return
     */
    public static boolean uploadFileStream(String uri, Configuration conf, InputStream stream, String remoteFSPath){
        try {
            FileSystem fs = FileSystem.get(URI.create(uri),conf);
            FSDataOutputStream hdOutStream = fs.create(new Path(remoteFSPath), true);
            IOUtils.copy(stream,hdOutStream);
            hdOutStream.flush();
            hdOutStream.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 上传本地文件到HDFS
     * @param uri
     * @param conf
     * @param localFilePath 本地文件路径
     * @param destPath
     * @return
     */
    public static boolean uploadFile(String uri, Configuration conf, String localFilePath, String destPath){
        try {
            FileSystem fs = FileSystem.get(URI.create(uri),conf);
            fs.copyFromLocalFile(new Path(localFilePath),new Path(destPath));
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 重命名文件
     * @param uri
     * @param conf
     * @param filePath 需要重命名的文件路径
     * @param newFilePath 重命名为
     * @return
     */
    public static boolean rename(String uri, Configuration conf, String filePath, String newFilePath) {
        try {
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            fs.rename(new Path(filePath), new Path(newFilePath));
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 从HDFS上下载文件到本地
     * @param uri
     * @param conf
     * @param remoteFSPath HDFS上文件的路径
     * @param localPath 保存到本地的路径
     * @return
     */
    public static boolean downloadFile(String uri, Configuration conf, String remoteFSPath, String localPath){
        try {
            FileSystem fs = FileSystem.get(URI.create(uri),conf);
            fs.copyToLocalFile(new Path(remoteFSPath),new Path(localPath));
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 下载HDFS文件到本地
     * @param uri
     * @param conf
     * @param remoteFSPath HDFS上文件的路径
     * @return
     */
    public static InputStream downloadFileStream(String uri, Configuration conf, String remoteFSPath){
        try {
            FileSystem fs = FileSystem.get(URI.create(uri),conf);
            FSDataInputStream stream = fs.open(new Path(remoteFSPath));
            return stream;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 查询HDFS上路径下所有文件的信息
     * @param uri
     * @param conf
     * @param dir HDFS上的路径
     * @param recursive 是否递归
     * @return
     */
    public static List<LocatedFileStatus> listFiles(String uri, Configuration conf, String dir, boolean recursive){
        try {
            FileSystem fs = FileSystem.get(URI.create(uri),conf);
            RemoteIterator<LocatedFileStatus> fileList = fs.listFiles(new Path(dir), recursive);
            List<LocatedFileStatus> files = new ArrayList<>();
            while (fileList.hasNext()){
                LocatedFileStatus file = fileList.next();
                files.add(file);
            }
            return files;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取HDFS集群上所有节点信息
     * @param uri
     * @param conf
     * @return
     */
    public static List<DatanodeInfo> getHostNames(String uri, Configuration conf) {
        try {
            DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(URI.create(uri), conf);
            DatanodeInfo[] dataNodeStats = fs.getDataNodeStats();
            return Arrays.asList(dataNodeStats);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 删除HDFS上的文件
     * @param uri
     * @param conf
     * @param dir 文件路径
     * @return
     */
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

    /**
     * 获得某个文件的状态
     * @param uri
     * @param conf
     * @param path 文件路径
     * @return
     */
    public static FileStatus getFileStatus(String uri, Configuration conf, String path){
        try {
            FileSystem fs = FileSystem.get(URI.create(uri),conf);
            return fs.getFileStatus(new Path(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
