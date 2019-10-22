package cn.doitedu.course.data;

import org.apache.commons.lang3.StringUtils;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.Random;
import java.util.UUID;

/**
 * @author: 余辉
 * @blog:   https://blog.csdn.net/silentwolfyh
 * @create: 2019/10/22
 * @description: 造数据：历史用户记录
 **/
public class HisuDataFactory {
    public static void main(String[] args) throws Exception {

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("data_ware/demodata/demo_hisu_dtl.csv")));

        Random rand1 = new Random();
        for(int i = 0;i<400;i++) {

            // 随机生成一个日期作为新增日期
            int firstDayInt = rand1.nextInt(30) + 1;

            String firstDay = StringUtils.leftPad(firstDayInt + "", 2, "0");

            // 随机生成一个整数，+上新增日期得到 末登日期
            int addDays = rand1.nextInt(31 - firstDayInt);
            int lastDayInt = firstDayInt + addDays;
            String lastDay = StringUtils.leftPad(lastDayInt + "", 2, "0");

            // 随机生成一个用户id
            String uid = UUID.randomUUID().toString().substring(0, 10);

            bw.write(uid+",2019-08-"+firstDay+",2019-08-"+lastDay);
            bw.newLine();

        }
        bw.close();

    }

}
