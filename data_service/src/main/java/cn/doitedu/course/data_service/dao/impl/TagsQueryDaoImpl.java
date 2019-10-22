package cn.doitedu.course.data_service.dao.impl;

import cn.doitedu.course.data_service.dao.ITagsQueryDao;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.HashMap;

@Repository
public class TagsQueryDaoImpl implements ITagsQueryDao {
    Connection conn = null;
    Table tblTags = null;
    Table tblIdx = null;

    @Value("${hbase.zk}")
    String zk;

    @Value("${hbase.tagtable}")
    String tagsTable;

    @Value("${hbase.idxtable}")
    String idxTable;


    /**
     * spring实例化该bean后会调用一次该方法
     * 用于初始化一些变量
     * @throws IOException
     */
    @PostConstruct
    public void init() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zk);
        conn = ConnectionFactory.createConnection(conf);
        tblTags = conn.getTable(TableName.valueOf(tagsTable));
        tblIdx = conn.getTable(TableName.valueOf(idxTable));

    }


    /**
     * spring在销毁该bean之前会调用一次该方法
     * 用于清理资源
     * @throws IOException
     */
    @PreDestroy
    public void clean() throws IOException {

        tblTags.close();
        tblIdx.close();
        conn.close();
    }



    @Override
    public HashMap<String, HashMap<String, Double>> getTagsByGid(String gid, String date) {

        HashMap<String, HashMap<String, Double>> resultMap = new HashMap<String, HashMap<String, Double>>();
        HashMap<String, Double> tagMap = null;
        try {
            String md5Gid = DigestUtils.md5Hex(gid).substring(0, 10) + date;
            tagMap = getTagsByMd5Gid(md5Gid);

        } catch (Exception e) {

        }

        resultMap.put(gid, tagMap);
        return resultMap;
    }


    public HashMap<String, Double> getTagsByMd5Gid(String md5Gid) {

        HashMap<String, Double> tagMap = new HashMap<>();
        try {
            Get get = new Get(md5Gid.getBytes());
            Result result = tblTags.get(get);

            CellScanner cellScanner = result.cellScanner();
            while (cellScanner.advance()) {
                Cell cell = cellScanner.current();
                byte[] q = CellUtil.cloneQualifier(cell);
                byte[] w = CellUtil.cloneValue(cell);
                String module_tag_value = new String(q);
                double weight = Bytes.toDouble(w);
                tagMap.put(module_tag_value, weight);
            }

        } catch (Exception e) {

        }

        return tagMap;
    }


    @Override
    public HashMap<String, HashMap<String, Double>> getTagsById(String id, String date) {
        HashMap<String, HashMap<String, Double>> resultMap = new HashMap<String, HashMap<String, Double>>();
        HashMap<String, Double> tagMap = null;

        try {
            Get get = new Get(id.getBytes());
            Result result = tblIdx.get(get);
            byte[] md5GidBytes = result.getValue("f".getBytes(), "q".getBytes());
            String md5Gid = new String(md5GidBytes);
            tagMap = getTagsByMd5Gid(md5Gid);




        } catch (IOException e) {
            e.printStackTrace();
        }
        resultMap.put(id,tagMap);

        return resultMap;
    }


}
