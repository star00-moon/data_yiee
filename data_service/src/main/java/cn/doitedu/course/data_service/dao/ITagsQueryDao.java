package cn.doitedu.course.data_service.dao;

import java.util.HashMap;

public interface ITagsQueryDao {

    HashMap<String, HashMap<String,Double>> getTagsByGid(String gid,String date);

    HashMap<String, HashMap<String,Double>> getTagsById(String id,String date);


}
