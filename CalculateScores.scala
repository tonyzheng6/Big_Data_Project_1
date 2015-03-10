/**
 * Title:       CalculateScores.scala
 * Created by:  Tony on 2/27/15.
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
 *              To compile: scalac *.scala // this doesn't work on glab but it should
 *              To run:     scala CalculateScores input.txt // the above doesn't work so this wouldn't either
 * Notes:       Concurrent version
 */

object CalculateScores 
{
  /**
   * Main method where command line argument is taken in and creates a new instance of Test class where all the
   * operations are performed
   */
  def main(args:Array[String]):Unit = 
  {
    val myTest = new Test(args(0))
    myTest.getK()
    myTest.run()
    //myTest.printAll()
    //myTest.printCategories()
    myTest.getStats()
    myTest.sortList()
  }
}
