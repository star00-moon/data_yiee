package cn.doitedu.course.data_service.service;

import org.springframework.web.bind.annotation.PathVariable;

import java.util.HashMap;

/**
 * @author: 余辉
 * @blog:   https://blog.csdn.net/silentwolfyh
 * @create: 2019/10/22
 * @description: 明细标签查询服务接口
 **/
public interface ITagsQueryService {

    HashMap<String, HashMap<String,Double>> getTagsByGid(String gid,String date);

    HashMap<String, HashMap<String,Double>> getTagsById(String id,String date);

    /**
     * 还可以写更多的条件查询功能，利用hbase的过滤器
     */


}
