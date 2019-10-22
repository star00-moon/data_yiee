object SeqDemo {

  def main(args: Array[String]): Unit = {

    val seq = Seq(1,2,3)
    val seq2 = seq.+:(4)
    val seq3 = seq.++:(Seq(6,6,6))
    val seq5 = seq.++(Seq(7,7,7))
    val seq4 = seq.:+(9)
    val x: String = seq.+("5")

    println(seq2)
    println(seq3)
    println(seq4)
    println(seq5)



  }

}
