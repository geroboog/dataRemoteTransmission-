package com.timesbigdata.sqlGen.preparer;

import com.timesbigdata.sqlGen.util.JDBCUtil;

import java.sql.PreparedStatement;

/**
 * Created by Link on 2017/5/12.
 */
public class CompanyDetailPreparer implements IPreparer {
    @Override
    public String getSQL(String tableName) {
        return "INSERT INTO TB_COMPANY_DETAIL (COMPANY_NAME,REGIST_NUM,COMPANY_TYPE,REGIST_ORG,CREATE_TIME) VALUES(?,?,?,?,?)";
    }
    @Override
    public String getDeleteSQL(String tableName) {
        return "DELETE from TB_COMPANY_DETAIL ";
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
        state=calculateState(split[3]);
        if(state==false)return state;
        state=calculateState(split[4]);
        if(state==false)return state;

        if(state==true)
        {

            JDBCUtil.preparedString(ps,1, split[0]);
            JDBCUtil.preparedString(ps,2, split[1]);
//        JDBCUtil.preparedString(ps,1, split[0]);
//        JDBCUtil.preparedString(ps,2, split[1]);
            JDBCUtil.preparedString(ps,3, split[2]);
            JDBCUtil.preparedString(ps,4, split[3]);
            JDBCUtil.preparedString(ps,5, split[4]);
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
