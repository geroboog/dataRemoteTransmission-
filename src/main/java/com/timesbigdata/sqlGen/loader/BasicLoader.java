package com.timesbigdata.sqlGen.loader;

import com.timesbigdata.sqlGen.preparer.IPreparer;
import com.timesbigdata.sqlGen.util.JDBCUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Link on 2017/5/11.
 */
public class BasicLoader extends Thread {
    private static Logger log = LoggerFactory.getLogger(BasicLoader.class);

    private static volatile AtomicInteger index = new AtomicInteger(0);

    protected IPreparer preparer;
    protected AtomicBoolean eof;
    protected ConcurrentLinkedQueue<String> lines;
    protected String jdbcUrl;
    protected String user,pwd;
    protected String tableName;
    protected Boolean useBatch;

    private AtomicInteger queueSize = new AtomicInteger(0);

    public BasicLoader(String tableName, IPreparer preparer, AtomicBoolean eof, String jdbcUrl, String user, String pwd,Boolean useBatch) {
        this.tableName = tableName;
        this.preparer = preparer;
        this.eof = eof;
        this.lines = new ConcurrentLinkedQueue<String>();
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.pwd = pwd;
        this.useBatch = useBatch;
    }

    @Override
    public void run() {
        setName("BasicLoader-" + index.getAndAdd(1));
        Connection conn = null;
        PreparedStatement ps = null;

        long rowNum = 0;

        try {
            conn = JDBCUtil.getConn(jdbcUrl,user,pwd);

            ps = conn.prepareStatement(preparer.getSQL(tableName));

            if(useBatch) conn.setAutoCommit(false);

            while (!(lines.isEmpty() && eof.get())) {
                String line = lines.poll();

                if(StringUtils.isBlank(line)) continue;
                queueSize.addAndGet(-1);

                Boolean state=preparer.preparedArgs(ps,line);
                if(state==false)
                {
                    continue;
                }
                rowNum++;

                try {
                    if(useBatch) {
                        ps.addBatch();
                        if( (rowNum) % 100 == 0 ) {
                            ps.executeBatch();
                            conn.commit();
                        }
                    } else {
                        ps.execute();
                    }
                } catch (SQLException ignored) {}
                if(rowNum % 10000 == 0) {
                    if(useBatch) {
                        ps.executeBatch();
                        conn.commit();
                    }
                    JDBCUtil.release(conn,ps,null);
                    conn = JDBCUtil.getConn(jdbcUrl,user,pwd);
                    ps = conn.prepareStatement(preparer.getSQL(tableName));
                }
            }
            if(useBatch) {
                ps.executeBatch();
                conn.commit();
            }
            log.info("{} done...",getName());
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JDBCUtil.release(conn,ps,null);
        }
    }

    public Integer getQueueSize() {
        return queueSize.get();
    }

    public void addToQueue(String line) {
        if(lines.add(line))
            queueSize.addAndGet(1);
    }

    public static List<BasicLoader> loaders(String tableName, IPreparer preparer, AtomicBoolean eof, String jdbcUrl, String user, String pwd,Integer number,Boolean useBatch) {
        List<BasicLoader> ret = new ArrayList<BasicLoader>();
        for(int i = 0 ; i < number ; i++) {
            ret.add(new BasicLoader(tableName,preparer,eof,jdbcUrl,user,pwd,useBatch));
        }
        return ret;
    }

    public static void startLoaders(List<BasicLoader> loaders) {
        for(BasicLoader loader : loaders) loader.start();
    }
}
