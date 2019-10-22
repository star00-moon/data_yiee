package cn.doitedu.course.data_service.service.impl;

import cn.doitedu.course.data_service.dao.ITagsQueryDao;
import cn.doitedu.course.data_service.service.ITagsQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;


@Service
public class TagsQueryServiceImpl implements ITagsQueryService {

    @Autowired
    ITagsQueryDao tagsQueryDao;

    @Override
    public HashMap<String, HashMap<String, Double>> getTagsByGid(String gid,String date) {


        HashMap<String, HashMap<String, Double>> tagsByGid = tagsQueryDao.getTagsByGid(gid, date);

        return tagsByGid;
    }

    @Override
    public HashMap<String, HashMap<String, Double>> getTagsById(String id,String date) {

        HashMap<String, HashMap<String, Double>> tagsById = tagsQueryDao.getTagsById(id, date);

        return tagsById;
    }
}
