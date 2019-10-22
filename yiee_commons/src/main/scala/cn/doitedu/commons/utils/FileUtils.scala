package cn.doitedu.commons.utils

import java.io.File

/**
 * @author: 余辉 
 * @blog:   https://blog.csdn.net/silentwolfyh
 * @create: 2019/10/22
 * @description: 删除文件目录及文件
 **/
object FileUtils {

  def deleteDir(dir: File): Unit = {
    val files = dir.listFiles()
    if(files == null){
      return
    }
    files.foreach(f ⇒ {
      if (f.isDirectory) {
        deleteDir(f)
      } else {
        f.delete()
//        println("delete file " + f.getAbsolutePath)
      }
    })
    dir.delete()
    println("delete dir " + dir.getAbsolutePath)
  }

  def main(args: Array[String]): Unit = {
    deleteDir(new File("user_profile/demodata/graphx/out_idmp"))
  }
}
