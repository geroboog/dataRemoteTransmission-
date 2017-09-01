package com.timesbigdata.sqlGen;

import com.timesbigdata.sqlGen.loader.BasicLoader;
import com.timesbigdata.sqlGen.preparer.IPreparer;
import com.timesbigdata.sqlGen.reader.BasicCSVReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by Link on 2017/5/12.
 */
public class Entrance extends Thread {
    private static Logger log = LoggerFactory.getLogger(Entrance.class);

    private List<BasicLoader> loaders;
    private BasicCSVReader reader;
    private AtomicBoolean eof;

    private String tableName;

    public Entrance(String filePath,String jdbcUrl,String user,String pwd, IPreparer preparer,Boolean titleRow,Boolean useBatch,Integer loaderNum) throws Exception {
        File inFile = new File(filePath);
        tableName = inFile.getAbsoluteFile().getName().replaceAll("\\..+$", "");
        this.eof = new AtomicBoolean(false);
        if(jdbcUrl.indexOf("mysql")>-1)
        {
            jdbcUrl = jdbcUrl.split("\\?")[0] + "?useUnicode=true&autoReconnect=true&failOverReadOnly=false";
        }else
        {
            jdbcUrl = jdbcUrl.split("\\?")[0] + " ";
        }

        if(useBatch)
            jdbcUrl += "&useServerPrepStmts=false&rewriteBatchedStatements=true";

        this.loaders = BasicLoader.loaders(tableName, preparer, eof, jdbcUrl, user, pwd, loaderNum,useBatch);
        reader = new BasicCSVReader(filePath,titleRow,loaders,eof);

        setName("Entrance-"+ tableName);
    }

    @Override
    public void run() {
        log.info("table name : {}",tableName);
        log.info("start Reader...");
        reader.start();
        log.info("start Loaders...");
        BasicLoader.startLoaders(loaders);

        int lastNum = 0;

        while (!eof.get()) {
            int currNum = reader.getLineNum();
            double speed = (currNum - lastNum) / 0.5;
            lastNum = currNum;
            log.info("loaded {} line , speed {} row(s) / s ",currNum,speed);
            try {
                sleep(500);
            } catch (InterruptedException e) {}
        }
    }
}
