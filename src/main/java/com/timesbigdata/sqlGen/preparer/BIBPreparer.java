package com.timesbigdata.sqlGen.preparer;

import com.timesbigdata.sqlGen.util.JDBCUtil;

import java.sql.PreparedStatement;

/**
 * Created by Link on 2017/5/12.
 */
public class BIBPreparer implements IPreparer {
    @Override
    public String getSQL(String tableName) {
        return "INSERT INTO TB_BIB (TITLE,CREATE_TIME,URL) VALUES(?,?,?)";
    }
    @Override
    public String getDeleteSQL(String tableName) {
        return "DELETE from TB_BIB ";
    }
    @Override
    public Boolean preparedArgs(PreparedStatement ps, String dataLine) {
        String[] split = dataLine.split(",");
        Boolean state=true;

        state=calculateState(split[0]);
        if(state==false)return state;
        state=calculateState(split[1]);
        if(state==false)return state;
        state=calculateState(split[2]);
        if(state==false)return state;

        if(state==true)
        {

            JDBCUtil.preparedString(ps,1, split[0]);
            JDBCUtil.preparedString(ps,2, split[1]);
//        JDBCUtil.preparedString(ps,1, split[0]);
//        JDBCUtil.preparedString(ps,2, split[1]);
            JDBCUtil.preparedString(ps,3, split[2]);
        }
        return state;
    }
    private Boolean calculateState(String splitStr)
    {
//        String last=splitStr.substring(splitStr.length()-1,splitStr.length());
        if(splitStr.indexOf(" ")>-1)
        {
            return false;
        }else
        {
            return true;
        }
    }
}
