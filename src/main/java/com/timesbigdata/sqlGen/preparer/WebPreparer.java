package com.timesbigdata.sqlGen.preparer;

import com.timesbigdata.sqlGen.util.MsgDigestUtil;

import java.sql.PreparedStatement;

import static com.timesbigdata.sqlGen.util.JDBCUtil.preparedFloat;
import static com.timesbigdata.sqlGen.util.JDBCUtil.preparedInt;
import static com.timesbigdata.sqlGen.util.JDBCUtil.preparedString;

/**
 * Created by Link on 2017/5/12.
 */
public class WebPreparer implements IPreparer {
    @Override
    public String getSQL(String tableName) {
        return "INSERT INTO " + tableName + " (social_id,latn_id,credit_level,credit_score,score_shenfen," +
                "score_anquan,score_xingwei,score_caifu,score_renmai,total_fee,net_flow,age_group,acc_nbr_yd," +
                "acc_nbr_kd,acc_nbr_gh,divide_market_name,star_sign,birthday,area,is_arrear_history," +
                "onlinetime_group,is_smz,cardcounts_group,card_activity,is_yhts,price_max_group,have_car," +
                "total_fee_group,banklevel,disc_level,qwzs,cbzs,city_counts) " +
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
    }

    @Override
    public Boolean preparedArgs(PreparedStatement ps, String dataLine) {
        String[] split = dataLine.split("\\|");

        try {
            preparedString(ps,1, MsgDigestUtil.getMD5(split[0]));                    // social_id
//                    preparedString(ps,1, split[0]);                    // social_id
            preparedString(ps,2,split[1]);                    // latn_id
            preparedString(ps,3,split[2]);                    // credit_level
            preparedFloat(ps,4,split[3]);                     // credit_score
            preparedFloat(ps,5, split[4]);                    // score_shenfen
            preparedFloat(ps,6, split[5]);                    // score_anquan
            preparedFloat(ps,7, split[6]);                    // score_xingwei
            preparedFloat(ps,8, split[7]);                    // score_caifu
            preparedFloat(ps,9, split[8]);                    // score_renmai
            preparedFloat(ps,10, split[9]);                   // total_fee
            preparedFloat(ps,11, split[10]);                  // net_flow
            preparedString(ps,12,split[11]);                  // age_group
            preparedString(ps,13,split[12]);                  // acc_nbr_yd
            preparedString(ps,14,split[13]);                  // acc_nbr_kd
            preparedString(ps,15,split[14]);                  // acc_nbr_gh
            preparedString(ps,16,split[15]);                  // divide_market_name
            preparedString(ps,17,split[16]);                  // star_sign
            preparedString(ps,18,split[17]);                  // birthday
            preparedString(ps,19,split[18]);                  // area
            preparedInt(ps,20,split[19]);                     // is_arrear_history
            preparedString(ps,21,split[20]);                  // onlinetime_group
            preparedInt(ps,22,split[21]);                     // is_smz
            preparedString(ps,23,split[22]);                  // cardcounts_group
            preparedInt(ps,24,split[23]);                     // card_activity
            preparedInt(ps,25,split[24]);                     // is_yhts
            preparedString(ps,26,split[25]);                  // price_max_group
            preparedString(ps,27,split[26]);                  // have_car
            preparedString(ps,28,split[27]);                  // total_fee_group
            preparedInt(ps,29,split[28]);                     // banklevel
            preparedString(ps,30,split[29]);                  // disc_level
            preparedFloat(ps,31, split[30]);                  // qwzs
            preparedFloat(ps,32, split[31]);                  // cbzs
            preparedInt(ps,33,split[32]);                     // city_counts
        } catch (Exception ignored) {}
        return true;
    }

    @Override
    public String getDeleteSQL(String tableName) {
        return null;
    }
}
