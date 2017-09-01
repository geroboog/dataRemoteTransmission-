package com.timesbigdata.sqlGen.preparer;

import com.timesbigdata.sqlGen.util.JDBCUtil;
import com.timesbigdata.sqlGen.util.MsgDigestUtil;

import java.sql.PreparedStatement;

/**
 * Created by Link on 2017/5/12.
 */
public class AccNbrPreparer implements IPreparer {
    @Override
    public String getSQL(String tableName) {
        return "INSERT INTO " + tableName + " (social_id,acc_nbr,type) VALUES(?,?,?);";
    }

    @Override
    public Boolean preparedArgs(PreparedStatement ps, String dataLine) {
        String[] split = dataLine.split("\\|");
        JDBCUtil.preparedString(ps,1, MsgDigestUtil.getMD5(split[0]));
        JDBCUtil.preparedString(ps,2, MsgDigestUtil.getMD5(split[1]));
//        JDBCUtil.preparedString(ps,1, split[0]);
//        JDBCUtil.preparedString(ps,2, split[1]);
        JDBCUtil.preparedString(ps,3, split[2]);
        return true;
    }

    @Override
    public String getDeleteSQL(String tableName) {
        return null;
    }
}
