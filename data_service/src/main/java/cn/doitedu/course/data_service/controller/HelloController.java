package cn.doitedu.course.data_service.controller;


import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@Controller
public class HelloController {

    @RequestMapping("/hello/{name}")
    @ResponseBody
    public String hello(@PathVariable String name){
        return "hello "+ name;
    }


    @RequestMapping("/index")
    public String index(){
        return "index.html";
    }
}
