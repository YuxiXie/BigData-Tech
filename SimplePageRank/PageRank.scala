package org.apache.spark.examples.graphx

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession


object PageRank {
    def main(args: Array[String]) {
        // 创建 SparkSession
        val spark = SparkSession
            .builder
            .master("local")
            .appName(s"${this.getClass.getSimpleName}")
            .getOrCreate()
        val sc = spark.sparkContext
        // 读入edges.txt文件，得到整个graph结构
        val graph = GraphLoader.edgeListFile(sc, "/usr/local/spark/spark-2.3.3-bin-hadoop2.7/data/graphx/edges.txt")
        // 执行PageRank算法
        val ranks = graph.pageRank(0.000001).vertices
        // 读入节点名称
        val nodes = sc.textFile("/usr/local/spark/spark-2.3.3-bin-hadoop2.7/data/graphx/vertices.txt").map {line =>
            val fields = line.split("\t")
            (fields(0).toLong, fields(1))
        }
        // 合并节点列表和ranks列表，得到节点名称和page-rank值的列表
        val display = nodes.join(ranks).map {
            case (id, (name, rank)) => (name, rank)
        }
        // 输出结果
        println(display.collect().mkString("\n"))
        
        spark.stop()
    }
}
