
val line = "INSERT INTO `t_md_areas` VALUES (110000, '北京市', 0, '北京', 1, 1, 116.22519738192517, 40.21952491712514, 116.23128, 40.22077, 116.39564503787867, 39.92998577808024);"

val arr = line.split(" ",-1)
println(arr(4))
println(arr(5))
println(arr(6))
println(arr(7))
println(arr(8))
println(arr(9))