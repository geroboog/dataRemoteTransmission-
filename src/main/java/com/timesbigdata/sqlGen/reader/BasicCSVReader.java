package com.timesbigdata.sqlGen.reader;

import com.timesbigdata.sqlGen.loader.BasicLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Link on 2017/5/12.
 */
public class BasicCSVReader extends Thread {
    private static Logger log = LoggerFactory.getLogger(BasicCSVReader.class);

    protected String filePath;
    protected Boolean titleRow;

    private FileInputStream fis;
    private InputStreamReader isr;
    private BufferedReader br;

    private AtomicInteger lineNum = new AtomicInteger(0);

    private List<BasicLoader> sink;
    private AtomicBoolean eof;

    private int sinkCount;
    private int toSink = 0;


    public BasicCSVReader(String filePath,boolean titleRow,List<BasicLoader> sink,AtomicBoolean eof) throws Exception {
        this.filePath = filePath;
        this.titleRow = titleRow;

        this.fis = new FileInputStream(filePath);
        this.isr = new InputStreamReader(fis,"utf-8");
        this.br = new BufferedReader(isr);

        this.sink = sink;
        this.eof = eof;

        this.eof.set(false);

        sinkCount = sink.size();
    }

    @Override
    public void run() {
        setName("BasicCSVReader");
        while (!eof.get()) {
            applyLines();
            long sleepLen = calcSleepLen();

            if (sleepLen > 0)
                doSleep(sleepLen);
        }
    }

    private void applyLines() {
        try {
            String line = br.readLine();
            if(line == null) {
                eof.set(true);
                return;
            }
            if(!(titleRow && lineNum.get() == 0))
                sink.get(toSink).addToQueue(line);
            toSink = (toSink + 1) % sinkCount;
            lineNum.addAndGet(1);
        } catch (IOException e) {
            log.warn("",e);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        br.close();
        isr.close();
        fis.close();
        super.finalize();
    }

    public Integer getLineNum() {
        return lineNum.get();
    }

    private Integer getSinkCapacityAvg() {
        Integer sum = 0;
        for(BasicLoader loader : sink) {
            sum += loader.getQueueSize();
        }
        return sum / sink.size();
    }

    private long calcSleepLen() {
        Integer avg = getSinkCapacityAvg();
        long sleepLen = 500;

        if(avg < 25) return 0;
        else if(avg >= 25 && avg < 50 ) sleepLen = 1; // 每秒最多生产1k条
        else if(avg >= 50 && avg < 75 ) sleepLen = 2; // 每秒最多生产500条
        else if(avg >= 75 && avg < 100) sleepLen = 5; // 每秒最多生产200条
        else if(avg >= 100 && avg < 150) sleepLen = 10; // 每秒最多生产100条
        else if(avg >= 150 && avg < 250) sleepLen = 100; // 每秒最多生产10条
        else if(avg >= 250 && avg < 500) sleepLen = 200; // 每秒最多生产5条

        return sleepLen;
    }

    private void doSleep(long sleepLen) {
        try {
            sleep(sleepLen);
        } catch (InterruptedException ignored) {}
    }

}
