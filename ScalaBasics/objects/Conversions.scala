package com.tekcrux.objects

object Conversions {
  
    //println("%f inch = %f santimeters".format(1.0, Conversions.inchesToCentimeters(1.0)))
    //println("%f gallon = %f liters".format(1.0, Conversions.gallonsToLiters(1.0)))
    //println("%f mile = %f kilometers".format(1.0, Conversions.milesToKilometers(1.0)))

	  def inchesToCentimeters(value: Double) = value * 2.54
	  def gallonsToLiters(value: Double) = value * 3.78541178
	  def milesToKilometers(value: Double) = value * 1.609344
}

object Test123 extends App {
  
   println("%f inch = %f centimeters".format(1.0, Conversions.inchesToCentimeters(1.0)))
   println("%f gallon = %f liters".format(1.0, Conversions.gallonsToLiters(1.0)))
   println("%f mile = %f kilometers".format(1.0, Conversions.milesToKilometers(1.0)))
    
}

