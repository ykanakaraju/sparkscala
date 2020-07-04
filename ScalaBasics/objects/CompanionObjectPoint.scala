package com.tekcrux.objects

object CompanionObjectPoint extends App {
   println(Point(3,4))
}

class Point(x: Int = 0, y: Int = 0) extends java.awt.Point(x, y)

object Point {
	 def apply(x: Int = 0, y: Int = 0) = new Point(x, y)
}

