package com.timesbigdata.sqlGen;

import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 获取导入记录
 */
public class FileLog {
    private Set<String> result;
    private String localPath;
    public FileLog(String localPath)
    {
        this.localPath=localPath;
        result=new HashSet<String>();
    }
    public void getFileLog() throws FileNotFoundException {
        File inFile = new File(localPath + "/fileImportLog.txt");
        String line="";
        try {
                FileInputStream fis = new FileInputStream(inFile);
                InputStreamReader isr = new InputStreamReader(fis, "utf-8");
                BufferedReader br = new BufferedReader(isr);
            while ((line = br.readLine()) != null) {
                result.add(line);
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public Boolean checkExist(String filename) {
        return result.contains(filename);
    }
    public void saveFileLog(String filename) throws IOException {
        File inFile = new File(localPath + "/fileImportLog.txt");
        String line="";
        filename=filename+"\r\n";
        OutputStream is = null;

        try {
            is=new FileOutputStream(inFile,true);
            OutputStreamWriter oStreamWriter = new OutputStreamWriter(is, "utf-8");
            oStreamWriter.append(filename);
            oStreamWriter.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            is.close();
            System.out.println("saveComplete:"+filename);
        }
    }
}  