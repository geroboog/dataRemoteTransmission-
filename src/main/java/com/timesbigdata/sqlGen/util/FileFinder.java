package com.timesbigdata.sqlGen.util;

/**
 * Created by Administrator on 2017/8/2.
 */

import java.io.File;
import java.util.ArrayList;
import java.util.List;
/**
 * 实现一个查找文件夹下所有文件的算法
 */
public class FileFinder {
        public List<String> filePathList;
        private String in;
    public FileFinder(String in)
    {
        filePathList=new ArrayList<String>();
        this.in=in;
    }
        public static void main(String[] args) {
            FileFinder ff=new FileFinder("C:\\Users\\Administrator\\Desktop\\test");
            ff.getFileName();
            System.out.print(ff.filePathList.toString());
         }

        public  void getFileName() {
            getFileNameInDirectory(this.in);
        }

    private  void getFileNameInDirectory(String path) {
        File f = new File(path);
        if (!f.exists()) {
            System.out.println(path + " not exists");
            return;
        }

        File fa[] = f.listFiles();
        for (int i = 0; i < fa.length; i++) {
            File fs = fa[i];
            String name = fs.getName();

            if (fs.isDirectory()) {
                String thisPath=fs.getPath();
                getFileNameInDirectory(thisPath);
            } else {
                if (name.indexOf("csv")>-1) {
                    filePathList.add(fs.getPath());
                }
            }

        }
    }
}
