package com.timesbigdata.sqlGen;

import com.timesbigdata.sqlGen.loader.BasicLoader;
import com.timesbigdata.sqlGen.preparer.IPreparer;
import com.timesbigdata.sqlGen.reader.BasicCSVReader;
import com.timesbigdata.sqlGen.util.JDBCUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by Link on 2017/5/12.
 */
public class DeleteEntrance {
    private static Logger log = LoggerFactory.getLogger(DeleteEntrance.class);

    private List<BasicLoader> loaders;
    private BasicCSVReader reader;
    private AtomicBoolean eof;

    private String tableName;

    public DeleteEntrance(String jdbcUrl, String user, String pwd, IPreparer preparer,Boolean useBatch) throws Exception {
        Connection conn = null;
        PreparedStatement ps = null;
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

        conn = JDBCUtil.getConn(jdbcUrl,user,pwd);

        ps = conn.prepareStatement(preparer.getDeleteSQL(""));

        ps.execute();

    }

}
