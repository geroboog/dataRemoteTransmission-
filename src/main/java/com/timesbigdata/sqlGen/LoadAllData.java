package com.timesbigdata.sqlGen;

import com.timesbigdata.sqlGen.preparer.BIBPreparer;
import com.timesbigdata.sqlGen.preparer.CompanyDetailPreparer;
import com.timesbigdata.sqlGen.preparer.CompanyPreparer;
import com.timesbigdata.sqlGen.util.FileFinder;
import com.timesbigdata.sqlGen.util.JDBCUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/8/2.
 */
public class LoadAllData {
    private static Logger log = LoggerFactory.getLogger(LoadBIB.class);

    public static void startLoadAllData(String args[])
    {
        if (args.length==1) {

            String in = args[0];
            try {
                FTPListAllFiles.startDownLoad("132.121.4.1111",21, "common", "common!123",in,"utf-8","/TargetCrawler/","csv");
            } catch (IOException e) {
                e.printStackTrace();
            }

            String jdbcUrl = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=132.121.1111.14)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=132.121.1111.13)(PORT=1521))(LOAD_BALANCE=no)(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=newods2)(FAILOVER_MODE=(TYPE=SELECT)(METHOD=BASIC))))";
            String user = "111111111";
            String pwd = "11111111";
            Boolean useBatch = false;//Boolean.parseBoolean(args[4]);
            Integer loaderNum = 1;//Integer.parseInt(args[5]);
            Connection conn =null;
            try {
                conn = JDBCUtil.getConn(jdbcUrl, user, pwd);
            }catch (Exception e)
            {
                e.printStackTrace();
            }
            if(conn!=null) {
                try {
                    FileFinder ff = new FileFinder(in);
                    ff.getFileName();
                    List<String> fileList = ff.filePathList;
                    FileLog fileLog = new FileLog(in);
                    fileLog.getFileLog();


                    for (String filePath : fileList) {

                        /**查看该文件是否已被导入**/
                        if (fileLog.checkExist(filePath)) {
                            continue;
                        }

                        File inFile = new File(filePath);
                        String line = "";
                        try {
                            FileInputStream fis = new FileInputStream(inFile);
                            InputStreamReader isr = new InputStreamReader(fis, "utf-8");
                            BufferedReader br = new BufferedReader(isr);
                            String[] params = null;
                            while ((line = br.readLine()) != null) {
                                params = line.split(",");
                                if (params != null && params.length > 2) {
                                    break;
                                }
                            }
                            /**该行为最后一行则退出**/
                            if (line == null) {
                                continue;
                            }
                            if (line.indexOf("标题") > -1 && line.indexOf("日期") > -1 && line.indexOf("url") > -1) {
                                new Entrance(filePath, jdbcUrl, user, pwd, new BIBPreparer(), true, useBatch, loaderNum).start();
                                fileLog.saveFileLog(filePath);
                            } else if (line.indexOf("注册号") > -1 && line.indexOf("企业名称") > -1 && line.indexOf("企业状态") > -1) {
                                new Entrance(filePath, jdbcUrl, user, pwd, new CompanyPreparer(), true, useBatch, loaderNum).start();
                                fileLog.saveFileLog(filePath);
                            } else if (line.indexOf("企业名称") > -1 && line.indexOf("注册号") > -1 && line.indexOf("企业类型") > -1 && line.indexOf("登记机关") > -1 && line.indexOf("登记时间") > -1) {
                                new Entrance(filePath, jdbcUrl, user, pwd, new CompanyDetailPreparer(), true, useBatch, loaderNum).start();
                                fileLog.saveFileLog(filePath);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } else {
            log.info("usage : [inputFilePath]");
        }
    }
    public static void main(String args[]) {
        LoadAllData.startLoadAllData(args);
    }
}
