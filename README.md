# 2019SRP
2019年SRP期间忙活的离线推荐算法（东拼西凑之W2V+KMeans+SlopeOne）

近期整理了一下代码，放上来备用。

我主要实现了baseline（weight slope one、item slope one）和kmeans（参照了MLlib的源码，性能相比自己最开始暴力cartesian的算法好多了！）

论文刚于2020年7月发表（期刊一般）

> 计算机与现代化《基于图嵌入的用户加权Slope One算法》：http://www.c-a-m.org.cn/CN/10.3969/j.issn.1006-2475.2020.08.011



**先挖个坑**：算法是否有结合流式数据处理（用flume、kafka、sparkstreaming和流式kmeans）的价值？



<br />

<br />

# **项目源代码目录（src.main.scala）大致结构**

- baseline -- 一些基线算法，有weight slope one、item slope one（对论文的复现）和bi-polor slope one。
- utils -- 一些工具类，有评价指标类、数据分割和处理类（数据读取的实现内嵌到各个算法的main()中了）
- EWSO.scala -- SRP论文提出的主要方法，结合三块积木（
- HyperGraph.scala -- 师兄写的图嵌入部分（我未参与）
- DeepWalk.scala -- 师兄写的图嵌入部分（我未参与）
- MyKmeans.scala -- 我参照MLlib中KMeans源码写的聚类算法（其实跟源码差不多，只是用Array代替DistanceWithNorm做了简化）