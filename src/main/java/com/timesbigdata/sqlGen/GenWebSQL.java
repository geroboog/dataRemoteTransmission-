package com.timesbigdata.sqlGen;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;

/**
 * Created by Link on 2017/5/5.
 */
public class GenWebSQL {

    private static Logger log = LoggerFactory.getLogger(GenWebSQL.class);

    public static void main(String args[]) {
        if(args.length==1) {
            String in = args[0];

            File inFile = new File(in);

            String name = inFile.getName();

            if (!inFile.exists()) {
                log.warn("{} not exists, application now exit...",inFile.getAbsolutePath());
                return;
            }

            String inPath = inFile.getAbsolutePath();
            log.info("loading {} ...", inPath);

            String tableName = name.replace(".txt","");
            String out = inFile.getParentFile().getAbsolutePath() + File.separator + tableName + ".sql";

            File outFile = new File(out);

            String outPath = outFile.getAbsolutePath();
            try {
                FileInputStream fis = new FileInputStream(inFile);
                InputStreamReader isr = new InputStreamReader(fis);
                BufferedReader br = new BufferedReader(isr);

                FileOutputStream fos = new FileOutputStream(outFile);
                OutputStreamWriter osw = new OutputStreamWriter(fos,"utf-8");
                int lineNum = 0;
                if(outFile.exists()) {
                    log.info("output file {} exist, try to delete it...", outPath);
                    boolean delete = outFile.delete();
                    if(!delete) {
                        log.error("delete output file {} fail, application now exit",outPath);
                    } else {
                        log.info("delete file {} success", outPath,inPath);
                    }
                }
                boolean create = outFile.createNewFile();
                if(!create) {
                    log.error("create file {} failed, application now exit", outPath);
                    return;
                } else {
                    log.info("create file {} success, now start process file {} to sql", outPath,inPath);
                }

                String line;
                while ((line = br.readLine()) != null) {
                    if(lineNum++ ==0) continue;

                    line = new String(line.getBytes(), Charset.forName("utf-8"));

                    try {
                        String[] split = line.split("\\|");

                        String[] columns = new String[split.length];

                        columns[0] = StringUtils.isBlank(split[0]) ? "NULL" : warpString(split[0]);        // social_id
                        columns[1] = StringUtils.isBlank(split[1]) ? "NULL" : warpString(split[1]);        // latn_id
                        columns[2] = StringUtils.isBlank(split[2]) ? "NULL" : warpString(split[2]);        // credit_level
                        columns[3] = StringUtils.isBlank(split[3]) ? "NULL" : split[3];                    // credit_score
                        columns[4] = StringUtils.isBlank(split[4]) ? "NULL" : split[4];                    // score_shenfen
                        columns[5] = StringUtils.isBlank(split[5]) ? "NULL" : split[5];                    // score_anquan
                        columns[6] = StringUtils.isBlank(split[6]) ? "NULL" : split[6];                    // score_xingwei
                        columns[7] = StringUtils.isBlank(split[7]) ? "NULL" : split[7];                    // score_caifu
                        columns[8] = StringUtils.isBlank(split[8]) ? "NULL" : split[8];                    // score_renmai
                        columns[9] = StringUtils.isBlank(split[9]) ? "NULL" : split[9];                    // total_fee
                        columns[10] = StringUtils.isBlank(split[10]) ? "NULL" : split[10];                 // net_flow
                        columns[11] = StringUtils.isBlank(split[11]) ? "NULL" : warpString(split[11]);     // age_group
                        columns[12] = StringUtils.isBlank(split[12]) ? "NULL" : warpString(split[12]);     // acc_nbr_yd
                        columns[13] = StringUtils.isBlank(split[13]) ? "NULL" : warpString(split[13]);     // acc_nbr_kd
                        columns[14] = StringUtils.isBlank(split[14]) ? "NULL" : warpString(split[14]);     // acc_nbr_gh
                        columns[15] = StringUtils.isBlank(split[15]) ? "NULL" : warpString(split[15]);     // divide_market_name
                        columns[16] = StringUtils.isBlank(split[16]) ? "NULL" : warpString(split[16]);     // star_sign
                        columns[17] = StringUtils.isBlank(split[17]) ? "NULL" : warpString(split[17]);     // birthday
                        columns[18] = StringUtils.isBlank(split[18]) ? "NULL" : warpString(split[18]);     // area
                        columns[19] = StringUtils.isBlank(split[19]) ? "NULL" : split[19];                 // is_arrear_history
                        columns[20] = StringUtils.isBlank(split[20]) ? "NULL" : warpString(split[20]);     // onlinetime_group
                        columns[21] = StringUtils.isBlank(split[21]) ? "NULL" : split[21];                 // is_smz
                        columns[22] = StringUtils.isBlank(split[22]) ? "NULL" : warpString(split[22]);     // cardcounts_group
                        columns[23] = StringUtils.isBlank(split[23]) ? "NULL" : split[23];                 // card_activity
                        columns[24] = StringUtils.isBlank(split[24]) ? "NULL" : split[24];                 // is_yhts
                        columns[25] = StringUtils.isBlank(split[25]) ? "NULL" : warpString(split[25]);     // price_max_group
                        columns[26] = StringUtils.isBlank(split[26]) ? "NULL" : warpString(split[26]);     // have_car
                        columns[27] = StringUtils.isBlank(split[27]) ? "NULL" : warpString(split[27]);     // total_fee_group
                        columns[28] = StringUtils.isBlank(split[28]) ? "NULL" : split[28];                 // banklevel
                        columns[29] = StringUtils.isBlank(split[29]) ? "NULL" : warpString(split[29]);     // disc_level
                        columns[30] = StringUtils.isBlank(split[30]) ? "NULL" : split[30];                 // qwzs
                        columns[31] = StringUtils.isBlank(split[31]) ? "NULL" : split[31];                 // cbzs
                        columns[32] = StringUtils.isBlank(split[32]) ? "NULL" : split[32];                 // city_counts

                        String dataLine = StringUtils.join(columns, ",");

                        osw.write(("INSERT INTO " + tableName + " VALUES(" + dataLine + ");\n"));
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }

                    if(lineNum % 100 == 0) {
                        log.info("processing {} line",lineNum);
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

            log.info("sqlGen done. out file name {}" , outPath);
        } else {

            System.out.println("usage : [inputFilePath]");
        }
    }

    private static String warpString(String str) {
        if(str.equals("''")) return "NULL";
        if(str.startsWith("'") && str.endsWith("'")) return str;
        return "'" + str + "'";
    }

}
