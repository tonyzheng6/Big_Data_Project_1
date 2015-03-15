import scala.actors.Actor
import scala.actors.Actor._

/**
 * Title:       Merge.scala
 * Authors:     Andrew Baumann, Tony Zheng
 * Modified on: 3/4/2015
 * Description: Given a stream of N data points: (score, category), find the top k points with the highest scores, and
 *              calculate the rank and statistical significance of categories of the top k points. Stream is the file
 *              input with score and category separated by a tab, a binary max heap stores the top k data points and
 *              stores the information pertaining to population size, success states in population, number of draws,
 *              and the number of successes in the top k data points. Once those values have been gathered, the hyper-
 *              geometric distribution is calculated for each category and merge sort is used to sort the categories
 *              based on the hyper-geometric distribution from highest to lowest.
 * Build with:  Scala IDE (Eclipse or IntelliJ) or using the following commands on the glab machines
 *              To compile: scalac *.scala
 *              To run:     scala CalculateScores input.txt
 * Notes:       Concurrent Version
 */

object Merge {

  var myStringList1, myStringList2, myStringList3, myStringList4:List[String] = List()
  var myCategoryList, myMod1CategoryList, myMod2CategoryList:List[Category] = List()
  var myMod3CategoryList, myMod4CategoryList:List[Category] = List()

  def combineLists():Unit = {
    var countOdd, countEven:Int = 0

    while(countOdd < myMod1CategoryList.size) {
      var tempname = myMod1CategoryList(countOdd).getName
      var tempcount = myMod1CategoryList(countOdd).getCount

      if(!myStringList2.contains(tempname)) {
        val myCategory = new Category()
        myCategory.setName(tempname)
        myCategory.setCount(tempcount)
        myCategoryList = myCategoryList :+ myCategory
      }
      else {
        while(countEven < myCategoryList.size) {
          myCategoryList(countEven).compare(myMod1CategoryList(countOdd))
          countEven += 1
        }
      }
      countEven = 0
      countOdd += 1
    }

    countOdd=0
    countEven=0

    while(countOdd < myMod3CategoryList.size) {
      var tempname = myMod3CategoryList(countOdd).getName
      var tempcount = myMod3CategoryList(countOdd).getCount

      if(!myStringList3.contains(tempname)) {
        val myCategory = new Category()
        myCategory.setName(tempname)
        myCategory.setCount(tempcount)
        myCategoryList = myCategoryList :+ myCategory
      }
      else {
        while(countEven < myCategoryList.size) {
          myCategoryList(countEven).compare(myMod3CategoryList(countOdd))
          countEven += 1
        }
      }
      countEven = 0
      countOdd += 1
    }

    countOdd=0
    countEven=0

    while(countOdd < myMod4CategoryList.size) {
      var tempname = myMod4CategoryList(countOdd).getName
      var tempcount = myMod4CategoryList(countOdd).getCount

      if(!myStringList4.contains(tempname)) {
        val myCategory = new Category()
        myCategory.setName(tempname)
        myCategory.setCount(tempcount)
        myCategoryList = myCategoryList :+ myCategory
      }
      else {
        while(countEven < myCategoryList.size) {
          myCategoryList(countEven).compare(myMod4CategoryList(countOdd))
          countEven += 1
        }
      }
      countEven = 0
      countOdd += 1
    }
  }

  /**
   * Concurrent merge sort
   */
  implicit def IntIntLessThan(x:Category, y:Category) = {
    x.getHypergeometricDistribution() > y.getHypergeometricDistribution()
  }

  def mergeSort[T](xs:List[T])(implicit pred:(T, T) => Boolean):List[T] = {
    val m = xs.length / 2

    if(m == 0) {
      xs
    }
    else {
      def merge(ls:List[T], rs:List[T], acc:List[T] = List()):List[T] = (ls, rs) match {
        case(Nil, _) => acc ++ rs
        case(_, Nil) => acc ++ ls
        case(l :: ls1, r :: rs1) =>
          if(pred(l, r)) {
            merge(ls1, rs, acc :+ l)
          }
          else {
            merge(ls, rs1, acc :+ r)
          }
      }
      val(l, r) = xs splitAt m
      merge(mergeSort(l), mergeSort(r))
    }
  }

  def Merging(x:List[Category], y:List[Category], z:List[Category], w:List[Category], q:List[String], e:List[String], r:List[String], t:List[String], f:List[Category]): Unit= {
    val hlactor, blactor = new Sorter

    myMod1CategoryList = x
    myMod2CategoryList = y
    myMod3CategoryList = z
    myMod4CategoryList = w
    myStringList1 = q
    myStringList2 = e
    myStringList3 = r
    myStringList4 = t
    myCategoryList = f

    myMod1CategoryList = myCategoryList.take(myCategoryList.size/2)
    myMod2CategoryList = myCategoryList.drop(myCategoryList.size/2)

    hlactor.start
    blactor.start
    hlactor ! ReaderMessage1
    blactor ! ReaderMessage2

    receive { case UniqueEndMessage => }
    receive { case UniqueEndMessage => }

    combineLists()
    myCategoryList = mergeSort(myCategoryList)

    println("Hypergeometric distribution for all categories in sorted order: ")
    for(x <- myCategoryList) {
      println(x.getName() + " Geometric : " + x.getHypergeometricDistribution())
    }
  }

  /**
   * sort class that extends actors to merge
   */
  class Sorter extends Actor {
    def act {
      while(true) {
        receive {
          case ReaderMessage2 =>
            myMod2CategoryList = mergeSort(myMod2CategoryList)
            reply { UniqueEndMessage }
            exit()
          case ReaderMessage1 =>
            myMod1CategoryList = mergeSort(myMod1CategoryList)
            reply { UniqueEndMessage }
            exit()
        }
      }
    }
  }
}