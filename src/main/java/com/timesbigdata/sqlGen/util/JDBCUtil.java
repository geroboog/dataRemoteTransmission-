package com.timesbigdata.sqlGen.util;

import org.apache.commons.lang3.StringUtils;

import java.sql.*;

/**
 * Created by Link on 2017/5/10.
 */
public class JDBCUtil {
    public static void release(Connection conn, Statement stm, ResultSet res) {
        if(res!=null) {
            try { res.close(); } catch (SQLException ignored) {}
        }
        if(stm!=null) {
            try { stm.close(); } catch (SQLException ignored) {}
        }
        if(conn!=null) {
            try { conn.close(); } catch (SQLException ignored) {}
        }
    }

    public static Connection getConn(String jdbcUrl,String user,String pwd) throws SQLException {
        try {
            Class.forName("oracle.jdbc.OracleDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return DriverManager.getConnection(jdbcUrl,user,pwd);
    }

    public static void preparedString(PreparedStatement ps, int i, String value) {
        try {
            ps.setString(i, StringUtils.isBlank(value) ? null : value);
        } catch (Exception e) {
            preparedNull(ps,i, Types.VARCHAR);
        }
    }

    public static void preparedFloat(PreparedStatement ps,int i,String value)  {
        if(StringUtils.isBlank(value)) {
            preparedNull(ps,i,Types.FLOAT);
        }
        try {
            ps.setFloat(i, Float.parseFloat(value));
        } catch (Exception e) {
            preparedNull(ps,i,Types.FLOAT);
        }
    }

    public static void preparedInt(PreparedStatement ps,int i,String value) {
        if(StringUtils.isBlank(value)) {
            preparedNull(ps,i,Types.INTEGER);
        }
        try {
            ps.setInt(i, Integer.parseInt(value));
        } catch (Exception e) {
            preparedNull(ps,i,Types.INTEGER);
        }
    }

    private static void preparedNull(PreparedStatement ps,int i,int type) {
        try {
            ps.setNull(i, type);
        } catch (SQLException ignored) {}
    }
}
