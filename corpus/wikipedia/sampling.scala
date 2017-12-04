import java.io._
import java.util.zip._

/**
  * 標準入力から Wikipedia TSV を読み込んで指定された行数まで出力する。一覧や曖昧さの回避ページは除外される。
  */
val inFile = new File(args(0))
val outFile = new File(args(1))
val maxLines = args(2).toInt
val is = new FileInputStream(inFile)
val in = new BufferedReader(new InputStreamReader(if(inFile.getName.endsWith(".gz")) new GZIPInputStream(is) else is, "UTF-8"))
val os = new FileOutputStream(outFile)
val out = new BufferedWriter(new OutputStreamWriter(if(outFile.getName.endsWith(".gz")) new GZIPOutputStream(os) else os, "UTF-8"))
Iterator.continually(in.readLine()).takeWhile(_ != null).map(line => (line, line.split("\t", 3).apply(1))).collect{
  case (line, title) if !title.contains("一覧") && !title.contains("曖昧さ回避") => line
}.take(maxLines).foreach(line => out.write(line + "\n"))
out.close()
in.close()