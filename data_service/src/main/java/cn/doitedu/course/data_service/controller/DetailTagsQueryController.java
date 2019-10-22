package cn.doitedu.course.data_service.controller;


import cn.doitedu.course.data_service.service.ITagsQueryService;
import cn.doitedu.course.data_service.service.impl.TagsQueryServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;

@RestController
public class DetailTagsQueryController {


    @Autowired
    ITagsQueryService tagsQueryService;


    @RequestMapping("/tags/bygid/{gid}/{date}")
    public HashMap<String, HashMap<String,Double>> getTagsByGid(@PathVariable String gid,@PathVariable String date){


        HashMap<String, HashMap<String, Double>> tagsByGid = tagsQueryService.getTagsByGid(gid,date);

        return  tagsByGid;
    }



    @RequestMapping("/tags/byid/{id}/{date}")
    public HashMap<String, HashMap<String,Double>> getTagsById(@PathVariable String id,@PathVariable String date){


        HashMap<String, HashMap<String, Double>> tagsById = tagsQueryService.getTagsById(id,date);

        return  tagsById;
    }


}
