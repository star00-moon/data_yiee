package cn.doitedu.commons.utils

import java.io.File

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/22
  * @description: 删除文件目录 及 目录下面的文件
  **/
object FileUtils {

  def deleteDir(dir: File): Unit = {
    // 1、列出 dir下面的文件名称 listFiles
    val files: Array[File] = dir.listFiles()

    // 2、files == null 则返回，不执行
    if (files == null) {
      return
    }

    // 3、files迭代
    files.foreach(f ⇒ {
      // 3-1、如果是目录则删除
      if (f.isDirectory) {
        deleteDir(f)
      } else {
        // 3-2、如果是文件则是删除
        f.delete()
        //        println("delete file " + f.getAbsolutePath)
      }
    })

    // 4、删除传入的目录
    dir.delete()

    // 5、打印传入的目录的名称 getAbsolutePath
    println("delete dir " + dir.getAbsolutePath)
  }

  def main(args: Array[String]): Unit = {
    deleteDir(new File("user_profile/demodata/graphx/out_idmp"))
  }
}
