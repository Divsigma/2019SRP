object MapValuesTest {
    class A {
      def this(f: String) = {
        this()
        this.f = f
      }
      var f: String = _
      override def toString = s"A(${f})"
    }

  def main(args: Array[String]): Unit = {

    val m = Map(1 -> new A("a"))
    println("-------------------------")
    println(s"m=$m")
    println("-------------------------")
    val m1 = m.mapValues { a => a.f = "b"; println("1:" + a.f); a }
    println(s"m=$m")
    println(s"m1=$m1")
    println(s"m=$m")
    println("-------------------------")
    m1.foreach { kv => kv._2.f = "c"; println("2:" + kv._2.f) }
    println("-------------------------")
    println(s"m1=$m1")
    println("-------------------------")
    // val m2 = m.map(x => {x._2.f = "d"; println("3: " + x._2.f); x})
    // val m2 = m.transform { (k, v) => v.f = "d"; println("3: " + v.f);v }
    m.map(x => {x._2.f = "d"; println("3: " + x._2.f); x})
    // m.transform { (k, v) => v.f = "d"; println("3: " + v.f);v }
    println("-------------------------")
    println(s"m=$m")
    // println(s"m1=$m1")
    // println(s"m2=$m2")
    println("-------------------------")


    val x = Map(1 -> "a")
    // x.map(x => (x._1, "b"))
    // x.transform { (k, v) => "b"}
    val x1 = x.transform { (k, v) => "b"}
    println(s"x=$x")
    println(s"x1=$x1")

    println("-------------------------")

    val y = Map(1 -> Array(1.0))
    val y1 = y.mapValues(x => {
      println(" -- call mapValues() -- ")
      x(0) *= 2
      x
    })
    //println(s"y=$y")
    y.foreach(x => println(x._2(0)))
    //println(s"y1=$y1")
    y1.foreach(x => println(x._2(0)))
    //println(s"y=$y")
    y.foreach(x => println(x._2(0)))
    //println(s"y1=$y1")
    y1.foreach(x => println(x._2(0)))

    println("-------------------------")

    val z = Map(1 -> 1.0)
    val z1 = z.mapValues(x => {
      println(" -- call mapValues() -- ")
      x * 2
    })
    println(s"z=$z")
    println(s"z1=$z1")
    println(s"z=$z")
    println(s"z1=$z1")

    println("-------------------------")

    val a = Map(1 -> 1.0)
    val a1 = a.transform( (k, v) => {
      println(" -- call transform() -- ")
      v * 2
    })
    println(s"a=$a")
    println(s"a1=$a1")
    println(s"a=$a")
    println(s"a1=$a1")

    println("-------------------------")

    val b = Map(1 -> 1.0)
    val b1 = b.map( x => {
      println(" -- call map() -- ")
      (x._1, x._2*2)
    })
    println(s"b=$b")
    println(s"b1=$b1")
    println(s"b=$b")
    println(s"b1=$b1")
  }
}
