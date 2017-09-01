package com.timesbigdata.sqlGen;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

/**
 * 列出FTP服务器上指定目录下面的所有文件
 */
public class FTPListAllFiles {
    public FTPClient ftp;
    public ArrayList<String> arFiles;
    private String localPath;
    private String encode;

    /**
     * 重载构造函数
     * @param isPrintCommmand 是否打印与FTPServer的交互命令
     * @param localPath
     */
    public FTPListAllFiles(boolean isPrintCommmand, String localPath, String encode){
        this.localPath=localPath;
        ftp = new FTPClient();
        this.encode=encode;
        arFiles = new ArrayList<String>();
        if(isPrintCommmand){
            ftp.addProtocolCommandListener(new PrintCommandListener(new PrintWriter(System.out)));
        }
    }

    /**
     * 登陆FTP服务器 
     * @param host FTPServer IP地址 
     * @param port FTPServer 端口 
     * @param username FTPServer 登陆用户名 
     * @param password FTPServer 登陆密码 
     * @return 是否登录成功
     * @throws IOException
     */
    public boolean login(String host,int port,String username,String password) throws IOException{
        this.ftp.connect(host,port);
        if(FTPReply.isPositiveCompletion(this.ftp.getReplyCode())){
            if(this.ftp.login(username, password)){
                this.ftp.setControlEncoding(encode);
                return true;
            }
        }
        if(this.ftp.isConnected()){
            this.ftp.disconnect();
        }
        return false;
    }

    /**
     * 关闭数据链接 
     * @throws IOException
     */
    public void disConnection() throws IOException{
        if(this.ftp.isConnected()){
            this.ftp.disconnect();
        }
    }

    /**
     * 递归遍历出目录下面所有文件 
     * @param pathName 需要遍历的目录，必须以"/"开始和结束 
     * @throws IOException
     */
    public void List(String pathName) throws IOException{
        if(pathName.startsWith("/")&&pathName.endsWith("/")){
            String directory = pathName;
            //更换目录到当前目录  
            this.ftp.changeWorkingDirectory(new String(pathName.getBytes(encode),"iso-8859-1"));
            FTPFile[] files = this.ftp.listFiles();
            for(FTPFile file:files){
                if(file.isFile()){
                    arFiles.add(directory+file.getName());
                }else if(file.isDirectory()){
                    List(directory+file.getName()+"/");
                }
            }
        }
    }

    /**
     * 递归遍历目录下面指定的文件名 
     * @param pathName 需要遍历的目录，必须以"/"开始和结束 
     * @param ext 文件的扩展名 
     * @throws IOException
     */
    public void List(String pathName,String ext) throws IOException{
        if(pathName.startsWith("/")&&pathName.endsWith("/")){
            String directory = pathName;
            //更换目录到当前目录  
            this.ftp.changeWorkingDirectory(reducePath(pathName));
            FTPFile[] files = this.ftp.listFiles();
            for(FTPFile file:files){
                if(file.isFile()){
                    if(file.getName().endsWith(ext)){
                        arFiles.add(directory+file.getName());
                        download(file,directory+file.getName());
                    }
                }else if(file.isDirectory()){
                    List(directory+file.getName()+"/",ext);
                }
            }
        }
    }
    public  void download(FTPFile ff,String filePath) throws IOException {
        // 根据绝对路径初始化文件
        File localFile = new File(this.localPath + "/" + ff.getName());
        if(!localFile.exists()) {
            // 输出流
            OutputStream is = new FileOutputStream(localFile);
            // 下载文件
            this.ftp.retrieveFile(reducePath(filePath), is);
            is.close();
        }
    }
    private String reducePath(String path) throws UnsupportedEncodingException {
        return new String(path.getBytes(encode),"iso-8859-1");
    }
    public static void startDownLoad(String ip,int port,String username,String password,String path,String encode,String pathName,String ext) throws IOException {
//        FTPListAllFiles f = new FTPListAllFiles(true,"E:/test");
//        if(f.login("132.121.4.103", 21, "common", "common!123")){
//            f.List("/TargetCrawler/","csv");
//        }
        FTPListAllFiles f = new FTPListAllFiles(true,path,encode);
        if(f.login(ip, port, username, password)){
            f.List(pathName,ext);
        }
        f.disConnection();
        List<String> fileList=f.arFiles;
        for(String fileName:fileList)
        {
            System.out.println(fileName);
        }
    }
    public static void main(String args[]) throws IOException {
        FTPListAllFiles.startDownLoad("132.121.4.103",21, "common", "common!123","E:/test","utf-8","/TargetCrawler/","csv");
    }

}  