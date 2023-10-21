import scala.annotation.tailrec
//task1
object add {
  def mySum( a:Int, b:Int ) : Int = {
    var sum:Int = 0
    sum = a + b
    return sum
  }
  val s = mySum(20, 21)
}


//task2
def pascalTriangle(column: Int, row: Int): Int = {
  @tailrec
  def loop(c0: Int, r0: Int, pred: Array[Int], cur: Array[Int]): Int = {
    cur(c0) = (if (c0 > 0) pred(c0 - 1) else 0) + (if (c0 < r0) pred(c0) else 0)

    if ((c0 == column) && (r0 == row)) cur(c0)
    else if (c0 < r0) loop(c0 + 1, r0, pred, cur)
    else loop(0, r0 + 1, cur, new Array(_length = r0 + 2))
  }

  if ((column == 0) && (row == 0)) 1
  else loop(0, 1, Array(1), Array(0, 0))
}


//task3
@tailrec
def balance(chars: List[Char]): Boolean = {
  @tailrec
  def balanced(chars: List[Char], open: Int): Boolean = {
    if (chars.isEmpty) open == 0
    else
      if (chars.head == '(') balanced(chars.tail,open+1)
      else
        if (chars.head == ')') open>0 && balanced(chars.tail,open-1)
        else balanced(chars.tail,open)
  }
  balanced(chars,0)

  balance(List('a', '(', ')'))
}

//task4
object higherorderfunctions
{
  def main(args:Array[String])
  {
    val a = Array(1, 2, 3, 4, 5)
    val squares = a.map(x => x*x)
    val sumOfSquares = squares.reduceLeft((x, y) => x + y)
    return sumOfSquares
  }
}

//task5
"sheena is a punk rocker she is a punk punk".split(" ").map(s => (s, 1)).groupBy(p => p._1).mapValues(v => v.length)
/*
the string is split into an array of strings using " " as a parameter.
the array of strings is mapped by converting every element to a pair of the form (s,1) and 1 is initial frequency of all elements
 it is then grouped and accessed by the first position.
 the length of each element is calculated
 */

"sheena is a punk rocker she is a punk punk".split(" ").map((_, 1)).groupBy(_._1).mapValues(v => v.map(_._2).reduce(_+_))
/*
the string is split into an array of strings using " " as a parameter.
it is mapped and grouped by the first position.
the values are added in binary
 */



//task6
def sqrt(x: Double) =
  if (x < 0) -x else x

  def isGoodEnough(guess: Double, x: Double) =
    sqrt(guess * guess - x) / x < 0.0001

  def improve(guess: Double, x: Double) =
    (guess + x / guess) / 2

  @tailrec
  def sqrtIter(guess: Double, x: Double): Double =
    if (isGoodEnough(guess, x)) guess
    else sqrtIter(improve(guess, x), x)

  def sqrt(x: Double) = sqrtIter(1.0, x)

  sqrt(2)
  sqrt(100)
  sqrt(1e-16)
  sqrt(1e60)
