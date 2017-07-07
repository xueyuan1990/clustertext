package utils

import java.util.regex.Pattern


import scala.collection.mutable.ArrayBuffer

/**
  * Created by linjiang on 2017/4/24.
  */
object StrUtil {

  def main(args: Array[String]): Unit = {

    println(splitWord("ni2思维1.2%3-2.1-a").mkString(","))
    println("：\t" + toSBC("："))
    println(toSBC("：") == ":")
    println("12==１２:" + (toSBC("１２") == "12"))
    println(splitZH("ab我3s-af我们dr.a我"))
    println(splitZH("absadf"))
    println("《 ischines=" + isChiness('《'))
    println("。 ischines=" + isChiness('。'))

    println(trimPunctuation("!@lajdfl@@@@slfj''lk;'"))

    val str = "#UserMemory i'm Design# (nihao0-1_)如何让好的\n\n体验更持久"

    println(str + " ///" + splitZhEnWord(str).mkString(" :: "))
  }

  def containChines(line: String): Boolean = {
    chinessCnt(line) > 0
  }

  def chinessCnt(line: String): Int = {
    line.toCharArray.filter(c => {
      '\u4E00' <= c && c <= '\u9FCB'
    }).length
  }

  def englishCnt(line: String): Int = {
    line.toCharArray.filter(c => {
      'a' <= c && c <= 'z' ||
        'A' <= c && c <= 'Z'
    }).length
  }

  def trimPunctuation(word: String): String = {
    val pattern = "[\\p{Punct}· ~（）『』【】；：’‘“”《》，。？/、… ]"
    word.trim.replaceFirst(s"^$pattern+", "").replaceFirst(pattern + "+$", "")
  }

  def toSBC(input: String): String = {
    val c: Array[Char] = input.toCharArray
    val d = c.map(r => {
      if (r == '\u3000') ' '
      else if (r < '\uFF5F' && r > '\uFF00') {
        (r - 65248).toChar
      }
      else r
    })
    new String(d)
  }

  def trimTailNumConsiderHead(input: String, tailNumMax: Int = 1, headChinesMin: Int = 2): String = {
    val ret = input.trim
    val n = tailNumCnt(ret)
    if (n <= tailNumMax) {
      val head = ret.substring(0, input.length - n).trim
      if (chinessCnt(head) >= headChinesMin &&  isChiness(head(head.length - 1))) {
        head
      } else ret
    } else {
      ret
    }
  }

  def trimTailNum(input: String, tailNumMax: Int = 1): String = {
    val ret = input.trim
    val n = tailNumCnt(ret)
    if (n <= tailNumMax) {
      ret.substring(0, input.length - n).trim
    } else {
      ret
    }
  }

  def tailNumCnt(input: String): Int = {
    if (!isEmpty(input)) input.reverse.takeWhile(c => '0' <= c && c <= '9').length
    else 0
  }

  def isEmpty(input: String): Boolean = {
    input == null || input.isEmpty
  }

  def splitWord(input:String):Array[String]={
    val pat = Pattern.compile("(\\w[\\-\\w]+\\w)|(\\d+[-.]?\\d+%?)|([a-zA-Z]+['-]?[a-zA-Z]+)")
    val in = input.toCharArray
    val ret = new ArrayBuffer[String]
    var i = 0
    val m = pat.matcher(input)
    var start = 0
    while(m.find()){
      var end = m.start()
      ret ++= input.substring(start,end).filter(isChiness(_)).map(_.toString)
      ret += m.group()
      start = m.end()
    }

    ret.toArray

  }

  def splitZH(kw: String): IndexedSeq[String] = {
    val couples = kw.zip(kw.substring(1)).zipWithIndex
    val ind = 0 +: couples.filter(p => {
      zh(p._1)
    }).map(_._2 + 1) :+ kw.length
    ind.zip(ind.tail).map(i => kw.substring(i._1, i._2))
  }

  private def zh(cs: (Char, Char)): Boolean = {
    isChiness(cs._1) && !isChiness(cs._2) ||
      isChiness(cs._2) && !isChiness(cs._1)
  }

  def splitZhEnWord(kw: String): IndexedSeq[String] = {
    val couples = kw.zip(kw.substring(1)).zipWithIndex
    val ind = 0 +: couples.filter(p => {
      zhEn(p._1)
    }).map(_._2 + 1) :+ kw.length
    ind.zip(ind.tail)
      .map(i => kw.substring(i._1, i._2))
      .filter(s => !(s.length == 1 && isWordSep(s(0))))
      .map(trimPunctuation(_))
  }

  private def zhEn(cs: (Char, Char)): Boolean = {
    isChiness(cs._1) && !isChiness(cs._2) ||
      isChiness(cs._2) && !isChiness(cs._1) ||
      isWordSep(cs._1) ||
      isWordSep(cs._2)
  }

  private def isWordSep(c: Char): Boolean = {
    " !#^*(){}\t\n[]\\_|\";:,<>/?· ~（）『』【】；：’‘“”《》，。？/、…".contains(c)
  }


  private def sameCharType(cs: (Char, Char)): Boolean = {
    cs._1.isLetterOrDigit && cs._2.isLetterOrDigit ||
      isChiness(cs._1) && isChiness(cs._2)
  }


  private def isAllDigit(str: String) = str.matches("[0-9]+")

  private def isAsciLetterOrDigit(c: Char) = 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9'

  private def isChiness(c: Char) = '\u4E00' <= c && c <= '\u9FCB'


}
