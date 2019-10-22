package cn.doitedu.course.demo

import ch.hsr.geohash.GeoHash

object GeoHashDemo {

  def main(args: Array[String]): Unit = {


    // "longtitude":118.26110459869567,"latitude":29.011145032306608
    var lng = 118.26110459869567
    var lat = 29.011145032306608

    var geohash = GeoHash.withCharacterPrecision(lat,lng,5).toBase32
    println(geohash)


    lng = 118.391104
    lat = 29.011145

    geohash = GeoHash.withCharacterPrecision(lat,lng,5).toBase32
    println(geohash)





  }

}
