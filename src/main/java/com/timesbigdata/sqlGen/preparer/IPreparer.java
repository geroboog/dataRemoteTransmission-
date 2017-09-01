package com.timesbigdata.sqlGen.preparer;

import java.sql.PreparedStatement;

/**
 * Created by Link on 2017/5/11.
 */
public interface IPreparer {
    String getSQL(String tableName);
    Boolean preparedArgs(PreparedStatement ps, String dataLine);

    String getDeleteSQL(String tableName);
}
