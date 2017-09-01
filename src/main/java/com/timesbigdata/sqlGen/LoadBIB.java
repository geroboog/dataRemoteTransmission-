package com.timesbigdata.sqlGen;

import com.timesbigdata.sqlGen.preparer.BIBPreparer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Link on 2017/5/10.
 */
public class LoadBIB {
    private static Logger log = LoggerFactory.getLogger(LoadBIB.class);

    public static void main(String args[]) {
        if(args.length==6) {
            String in = args[0];
            String jdbcUrl = args[1];
            String user = args[2];
            String pwd = args[3];
            Boolean useBatch = Boolean.parseBoolean(args[4]);
            Integer loaderNum = Integer.parseInt(args[5]);

            try {
                new Entrance(in,jdbcUrl,user,pwd,new BIBPreparer(),true,useBatch,loaderNum).start();
            } catch (Exception e) {
                e.printStackTrace();
            }

        } else {
            log.info("usage : [inputFilePath] [jdbcUrl] [user] [password] [useBatch] [loaderNum]");
        }
    }
}
