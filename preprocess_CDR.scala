import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat
import scala.collection.mutable.ArrayBuffer
import java.math.BigDecimal
import scala.collection.mutable.Set
import scala.util.control._
import java.util.Date

object preprocess_CDR {
  def main(arg:Array[String]):Unit= {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val readli1=sc.textFile("/user/tele/trip/SplitData/HaiNan/201509/201509MM.csv")
    //val peo_record= readli1.map(_.split(","))
    val peo_random1000 = sc.textFile("/user/student/wj/tourist_notlocal1_1000.txt").map(_.split(",")).map(x=>x(0)).collect()                 //从文件导入待处理用户列表，进行筛选
    //val peo_list = peo_random1000.map(_.split(",")).map(x => x(0)).take(200)
    val peo_record= readli1.map(_.split(",")).filter(x => peo_random1000.contains(x(3)))
    val peo_record_pair = peo_record.filter(x=>x.length==5).map(x => (x(4),(x(3),x(1))))
    val cid_sameloc_table = sc.textFile("/user/student/fy/cid_sameloc_table.txt").map(_.split(",")).map(x => (x(0),x(1)))
    val peo_record_map_cid = peo_record_pair.leftOuterJoin(cid_sameloc_table)
    val peo_record_map_cid_pair = peo_record_map_cid.filter(x => x._2._2 != None).map(x => (x._2._2.get, (x._2._1._1,x._2._1._2)))
    val cid_table = sc.textFile("/user/tele/trip/BasicInfo/BaseStation/HaiNan.csv")
    val cid_pair = cid_table.map(_.split(",")).filter(x => x.length==14).map(x => (x(3),(x(7),x(12),x(13))))
    val peo_record_with_axis = peo_record_map_cid_pair.leftOuterJoin(cid_pair)
    val peo_record_with_axis_flat = peo_record_with_axis.map(x => x._2._1._1+","+x._2._1._2+","+x._1+","+getValue(x._2._2))
    val peo_groups = peo_record_with_axis_flat.map(_.split(",")).filter(x => x.length == 6).map(x => (x(0),(x(1),x(2),x(4),x(5).substring(0,x(5).length-1),x(0)))).groupByKey()
    //键值对（用户，（时间戳，cid,经度，纬度，用户）），以用户分组，并利用长度滤除包含查不到经纬度的基站的记录
    val sorted_peo_groups = peo_groups.mapValues(sortRecord(_))
    val sorted_peo_groups_1=sorted_peo_groups.filter(x=>x._2.toArray.length>200)
    val cidPair_df_normal_bid_pair_table = sc.textFile("/user/student/fy/cidPair_df_normal_bid_pair_table.txt").collect()
    var cidPair_df_normal_bid_pair_map:scala.collection.mutable.Map[String,String]=scala.collection.mutable.Map()
    for (pair <- cidPair_df_normal_bid_pair_table){
      cidPair_df_normal_bid_pair_map += (pair.split("\t")(0) -> pair.split("\t")(1))
    }
    def adjacentOrNot(cid1:String,cid2:String):Long = {
      if (cidPair_df_normal_bid_pair_map.contains(cid1)){
        val cid_list = cidPair_df_normal_bid_pair_map.get(cid1).get.split(",") //单独对一个map对象使用get方法，获取到的是一个Option类，获取里面的数值，需要再次调用get方法
        if (cid_list.contains(cid2)){
          1
        }else{
          0
        }
      }else{
        0
      }
    }
    val combined_peo_groups = sorted_peo_groups_1.mapValues(recordCombine2(_)).filter(x=>x._2.toArray.length>1)
    def deleteDrift(record: Iterable[(String,String,String,String,String,String,String)]): Array[String] = {

      val result = new ArrayBuffer[String]();

      val loop = new Breaks;

      //先以drift_time为阈值切割记录，将记录分成不同的段，在每一段内分析是否存在漂移现象
      //这个时间阈值很关键 如果过大，会导致很多记录被当作漂移数据去除，如果过小，记录的精度没有这么高
      //单独分到一段的记录如果段内记录过少是不会进行漂移分析的，因此如果某用户存在每隔二十分钟左右的记录，在drift_time等于30时会被分到同一段，
      //又由于速度过高（可能乘坐交通工具）距离过远而被判定为离群簇，是会被删除的。但是如果阈值为15就不会。
      val drift_time = 1200

      val record_list = record.toList            //List[(String,String,String,String,String,String,String)],每条记录:（cid，经度，纬度，用户，起始时间，截止时间，停留时间）
      val record_seg = ArrayBuffer[ArrayBuffer[(String,String,String,String,String,String,String)]]()             //存放所有段的数组缓冲，里面的每一个数组缓冲是一个段

      var temp_start = 0                                                     //每段起始指针
      var temp_end = 0                                                       //每段终止指针

      var j = 0
      for ( j <- 1 until record_list.length){                                //将全部记录分段
        if ((formatDate(record_list(j)._5)- formatDate(record_list(j-1)._6)) < drift_time){               //如果j对应记录起始时间与上一条记录截止时间的时间差小于1850
          temp_end = j                                                   //加入同一段，段终止指针+1
        }else{                                                                                            //如果时间差大于1850，切断
          var k = 0
          val temp =  new ArrayBuffer[(String,String,String,String,String,String,String)]()
          for (k <- temp_start to temp_end){                             //将j前的段的起始指针与终止指针之间的记录写入temp
            temp += record_list(k)
          }
          record_seg += temp
          temp_start = j                                                //j作为新段起始指针
          temp_end = j
        }
      }
      var k = 0
      val temp =  new ArrayBuffer[(String,String,String,String,String,String,String)]();
      for (k <- temp_start to temp_end){                                    //j到达记录末尾后，写入最后一段
        temp += record_list(k)
      }
      record_seg += temp


      //去除漂移数据的阈值
      val ms = 3        //距离比阈值ms:di,i+1/di,i+2
      val ns = 3        //距离比阈值ns:di,i+1/di+1,i+2
      val ds = 4        //距离阈值ds
      val vs = 60       //速度阈值vs  m/s

      val rec_num_percent = 0.34     //离群簇记录数占段记录数比例阈值
      val cid_num = 3                //离群簇判定基站数目阈值
      val ts = 10                    //离群簇判定时间阈值

      val seg_length = 3             //进行漂移分析的段内最少记录数

      var seg = new ArrayBuffer[(String,String,String,String,String,String,String)]();

      for (seg <- record_seg.toArray){
        val seg_record = seg.toArray                    //seg_record:Array[(String,String,String,String,String,String,String)]

        if (seg_record.length >= seg_length){                    //如果段内记录数大于等于3，则进行漂移分析，如果小于就不分析直接写入结果
          //result += "seg_length>3_begin"
          var cluster_begin = 0                         //簇起始指针，初始化为第一个
        var cluster_end = 0                           //簇终止指针，初始化为第一个
        var drift_id = new ArrayBuffer[Int]();        //簇内单独离群点id数组

          loop.breakable {
            while (cluster_end < seg_record.length-1){
              //簇end+1与簇end的时间差,距离，速度
              var time_difference_i01 = formatDate(seg_record(cluster_end+1)._5)-formatDate(seg_record(cluster_end)._6)
              var location_difference_i01 = getDistance(seg_record(cluster_end+1)._2.toDouble,seg_record(cluster_end+1)._3.toDouble,seg_record(cluster_end)._2.toDouble,seg_record(cluster_end)._3.toDouble)
              var speed_i01 = 0.0
              if (time_difference_i01 != 0 && location_difference_i01 != 0.0){
                speed_i01 = location_difference_i01*1000/time_difference_i01                //单位：m/s
              }
              //簇end+1与簇end是否邻近
              var adjacent = adjacentOrNot(seg_record(cluster_end+1)._1,seg_record(cluster_end)._1)

              //如果簇end+1与簇end不临近并且距离超过阈值
              if (adjacent == 0 && location_difference_i01 > ds){
                //判定簇end+1为离群点还是与簇end+2同属离群簇

                //先判断簇end+2是否存在（end+1是否到结尾）
                if (cluster_end +2 == seg_record.length){    //如果end+1到了结尾
                  if (speed_i01 < vs){   //如果end+1不离群，簇终止指针加一
                    cluster_end += 1
                  }
                  //如果end+1离群，簇终止指针不变
                  //跳出判定
                  loop.break;
                }else{                                      //如果end+1没到结尾，end+2存在
                  //簇end+1与簇end+2的时间差,距离，速度
                  var time_difference_i12 = formatDate(seg_record(cluster_end+2)._5)-formatDate(seg_record(cluster_end+1)._6)
                  var location_difference_i12 = getDistance(seg_record(cluster_end+1)._2.toDouble,seg_record(cluster_end+1)._3.toDouble,seg_record(cluster_end+2)._2.toDouble,seg_record(cluster_end+2)._3.toDouble)
                  var speed_i12 = 0.0
                  if (time_difference_i12 != 0 && location_difference_i12 != 0.0){
                    speed_i12 = location_difference_i12*1000/time_difference_i12                //单位：m/s
                  }
                  //簇end与簇end+2距离
                  var location_difference_i02 = getDistance(seg_record(cluster_end+2)._2.toDouble,seg_record(cluster_end+2)._3.toDouble,seg_record(cluster_end)._2.toDouble,seg_record(cluster_end)._3.toDouble)

                  if (location_difference_i02 == 0){        //如果end与end+2为同一个点
                    drift_id += (cluster_end+1)           //移除end+1，结束指针后移两位
                    cluster_end += 2
                  }else{
                    //两种不同情况的距离比，m用于判定单独离群点，n用于判定离群簇
                    var m = location_difference_i02 / location_difference_i02
                    var n = location_difference_i02 / location_difference_i12

                    if (m > ms && speed_i01 * speed_i12 > vs * vs){                   //如果簇end+1为单独离群点，将簇end+1写入离群表
                      drift_id += (cluster_end+1)
                      cluster_end += 2                                              //簇end指针后移2位
                    }else if (n > ns && location_difference_i12 < ds){                //如果簇end+1与簇end+2同属离群簇，切断簇end，分析簇end是否离群，
                      //result += "cut_2"
                      result ++= resultWrite(cluster_begin,cluster_end,drift_id,seg_record,rec_num_percent,ts,cid_num)
                      //result += "cut_2_end"
                      cluster_begin = cluster_end+1                                 //并将新簇起始设为原始end+1，终止设为原始end+2
                      cluster_end = cluster_begin+1
                    }else{                                                            //如果都不满足，判定簇end是否离群，删除end+1，新簇从簇end+2开始
                      //result += "other"
                      //result += "loc_dif:"+location_difference_i01
                      result ++= resultWrite(cluster_begin,cluster_end,drift_id,seg_record,rec_num_percent,ts,cid_num)
                      //result += "other_end"
                      cluster_begin = cluster_end+2
                      cluster_end = cluster_begin
                    }
                  }
                }
              }else{                            //如果临近或者距离没有超过阈值：将簇end指针后移加1
                cluster_end += 1
              }
            }

          }
          //result += "last_cluster"
          result ++= resultWrite(cluster_begin,cluster_end,drift_id,seg_record,rec_num_percent,ts,cid_num)        //在簇终止指针到达记录结尾时，判定并写入最后一个簇
          //result += "last_cluster_end"
          //result += "seg_length>3_end"
        }else{                                   //如果段内记录数小于3，将段内记录直接写入result
          /*             var rec = new Array[String](7)
                      for (rec <- seg_record){
                          result += tupleToStr(rec)
                      } */
          var l = 0
          for (l <- 0 to seg_record.length-1){
            //result += "seg_length<3"
            result += tupleToStr(seg_record(l))
            //result += "seg_length<3_end"

          }
          //seg_record.foreach{x => result += tupleToStr(x)}
        }
      }

      result.toArray
    }
    def process_data(trajectory:Array[(String,String,String,String,String,String,String)]):Array[(String,String,String,String,String,String,String,String)]={
        val data_delete_drift = deleteDrift(trajectory)
        val data_temp = (data_delete_drift.map(_.split(","))).map(x => (x(0), x(1), x(2), x(3), x(4), x(5), x(6)))
      if(data_temp.length>100){
        val data_process_pingpong = pingpong(data_temp)
        val trajectory_result = trajectory_merge(data_process_pingpong)
        return trajectory_result
      }
      else{
        return Array[(String,String,String,String,String,String,String,String)]()
      }
    }
    val combined_peo_groups_1=combined_peo_groups.map(x=>(x._1,x._2.toArray))
    val combined_peo_groups_processed= combined_peo_groups_1.mapValues(process_data(_)).filter(x=>x._2.length>1)
    val result=combined_peo_groups_processed.flatMap(x=>x._2)
    result.repartition(1).saveAsTextFile("wj/201509_tourist_notlocal1_1000")
  }

  def time_sort(track_change:Array[(String,Long,String,String,String,String,String,String,String)]):Array[(String,Long,String,String,String,String,String,String,String)]={
    val track_sorted=track_change.sortBy(x=>x._2)
    return track_sorted
  }
  def timeToSec(time: String) = {
    val fm = new SimpleDateFormat("yyyyMMddHHmmss")
    val tm = time
    val dt = fm.parse(tm)
    dt.getTime() / 1000
  }
  def get_distance(Lon_1:String,Lat_1:String,Lon_2:String,Lat_2:String):Double={
    val r:Double =6370.99681
    val pi:Double=3.1415926
    val a1=(Lat_1.toDouble * pi) /180.0
    val a2=(Lon_1.toDouble * pi) /180.0
    val b1=(Lat_2.toDouble * pi) /180.0
    val b2=(Lon_2.toDouble * pi) /180.0

    var t1:Double=Math.cos(a1)*Math.cos(a2)*Math.cos(b1)*Math.cos(b2)
    var t2:Double=Math.cos(a1)*Math.sin(a2)*Math.cos(b1)*Math.sin(b2)
    var t3:Double=Math.sin(a1)*Math.sin(b1)
    val distance:Double=Math.acos(t1+t2+t3)*r
    return distance
  }
  def get_timediff(t1:String,t2:String):Double={
    val time_1=timeToSec(t1)
    val time_2=timeToSec(t2)
    val time_diff=(time_2-time_1).toDouble
    return time_diff
  }
  def getValue(x: Option[(String,String,String)]) = x match {                                  //模式匹配函数，能够得到option包含的值
    case Some((s)) => s
    case None => "?"          //有的基站在表里查不到经纬度，例如36075，用？代替
  }
  def sortRecord(record: Iterable[(String, String,String,String,String)]): Iterable[(String, String,String,String,String)] = {
      return record.toList.sorted  //排序函数：将每个用户组内记录转化成元组列表（时间戳，cid，经度，纬度）并排序
  }
  //基站距离计算（使用经纬度）
  def getDistance(_lon1: Double, _lat1: Double, _lon2: Double, _lat2: Double): Double = {
    if (_lat1==_lat2 && _lon1 == _lon2){
      0
    }else{
      val lat1: Double = (Math.PI / 180) * _lat1
      val lat2: Double = (Math.PI / 180) * _lat2
      val lon1: Double = (Math.PI / 180) * _lon1
      val lon2: Double = (Math.PI / 180) * _lon2
      //地球半径
      val R: Double = 6371
      val d: Double = Math.acos(Math.sin(lat1) * Math.sin(lat2) + Math.cos(lat1) * Math.cos(lat2) * Math.cos(lon2 - lon1)) * R
      new BigDecimal(d).setScale(4, BigDecimal.ROUND_HALF_UP).doubleValue;
    }
  }
  def formatDate(date: String): Long = {
    val fm = new SimpleDateFormat("yyyyMMddHHmmss")
    val tm = date
    val dt = fm.parse(tm)
    dt.getTime() / 1000
  }
  def recordCombine2(record: Iterable[(String, String,String,String,String)]): Iterable[(String,String,String,String,String,String,String)]  ={       //（时间戳，cid,经度，纬度，用户）转化成（cid，经度，纬度，用户，起始时间，截止时间，停留时间）
    val record_list = record.toList
    val result = new ArrayBuffer[(String,String,String,String,String,String,String)]();
    var temp_start = record_list(0)
    var temp_end = record_list(0)

    if(record_list.length>=2){
    for (i <- 1 until record_list.length) {
      if (record_list(i)._2 == temp_start._2 && formatDate(record_list(i)._1) - formatDate(record_list(i - 1)._1) < 1850) {
        temp_end = record_list(i)
      } else {
        val t = new Tuple7(temp_start._2, temp_start._3, temp_start._4, temp_start._5, temp_start._1, temp_end._1, (formatDate(temp_end._1) - formatDate(temp_start._1)).toString())
        result += t
        temp_start = record_list(i)
        temp_end = record_list(i)
      }
    }
    }
    result.toList
  }
  def tupleToStr(tup:(String,String,String,String,String,String,String)):String ={
    var rs = ""
    tup.productIterator.foreach(v=> rs = rs + v.toString+",")
    rs.substring(0,rs.length-1)
  }
  def resultWrite(cluster_begin:Int, cluster_end:Int, drift_id:ArrayBuffer[Int],seg_record:Array[(String,String,String,String,String,String,String)],rec_num_percent:Double,ts:Int,cid_num:Int): Array[String] = {
    var cluster_rec_num_percent = (cluster_end-cluster_begin)/seg_record.length        //求出簇记录数占段总记录数的比例
    var cluster_time = formatDate(seg_record(cluster_end)._6)-formatDate(seg_record(cluster_begin)._5)
    var cluster_cid_set = Set[String]()
    var cluster_write = new ArrayBuffer[String]();

    for (t <- cluster_begin to cluster_end){                      //统计簇内不重复基站数
      if (! drift_id.toArray.contains(t)){
        cluster_cid_set.add(seg_record(t)._1)
      }
    }

    var cluster_cid_num = cluster_cid_set.size

    if (cluster_rec_num_percent < rec_num_percent && cluster_time < ts && cluster_cid_num < cid_num){
      //离群簇不写入,这里先简单的利用“与”合并三个条件
      /* cluster_write += "abnormal_cluster"
      cluster_write += cluster_rec_num_percent+","+cluster_time+","+cluster_cid_num
      for (t <- cluster_begin to cluster_end){

          cluster_write += tupleToStr(seg_record(t))

      }
      cluster_write += "abnormal_cluster_end" */
    }else{
      //cluster_write += "normal_cluster"
      var t = 0
      for (t <- cluster_begin to cluster_end){
        if (! drift_id.toArray.contains(t)){                  //如果id不在离群点表里，写入
          //cluster_write += "normal"
          cluster_write += tupleToStr(seg_record(t))
        }
        /* else{
            cluster_write += "drift_rec"
            cluster_write += tupleToStr(seg_record(t))
        }  */
      }
      //cluster_write += "normal_cluster_end"
    }
    cluster_write.toArray
  }
  def compute_score(subtrajectory:Array[(String,String,String,String,String,String,String)],base_stations:Array[(String,String,String)]): Array[(String,String,String,Double)] ={
    var score_buffer=ArrayBuffer[(String,String,String,Double)]()
    for (i<- 0 until base_stations.length){
      var base_station=base_stations(i)._1
      var distance_average:Double=0
      var frequence=0
      for (j <-0 until subtrajectory.length){
        if(subtrajectory(j)._1!=base_station){
          distance_average=distance_average+get_distance(base_stations(i)._2,base_stations(i)._3,subtrajectory(j)._2,subtrajectory(j)._3)
        }
        else{
          frequence=frequence+1
        }
      }
      distance_average=distance_average/(subtrajectory.length-frequence)
      var score:Double=frequence/distance_average
      score_buffer+=((base_station,base_stations(i)._2,base_stations(i)._3,score))
    }
    score_buffer.toArray
  }
  //（3，开始时间，触发条件，用户ID，基站编号，基站名称，覆盖范围，经度，纬度，结束时间，停留时间，距离上一个基站的距离，到达该基站的速度）
  //(cid，经度，纬度，用户，起始时间，截止时间，停留时间）
  def pingpong(trajectory:Array[(String,String,String,String,String,String,String)]): Array[(String,String,String,String,String,String,String)]={
    var result_trajectory=ArrayBuffer[(String,String,String,String,String,String,String)]()
    var temp_trajecteory=ArrayBuffer[(String,String,String,String,String,String,String)]()
    var temp_station=ArrayBuffer[(String,String,String)]()
    temp_trajecteory+=((trajectory(0)._1,trajectory(0)._2,trajectory(0)._3,trajectory(0)._4,trajectory(0)._5,trajectory(0)._6,trajectory(0)._7))
    temp_station+=((trajectory(0)._1,trajectory(0)._2,trajectory(0)._3))
    //    var start_point:Int=0
    //    var end_point:Int=1
    for (i<-1 until trajectory.length){
      if(get_timediff(trajectory(i-1)._6.toString,trajectory(i)._5.toString)<300){
        //println(get_timediff(trajectory(i)._2.toString,trajectory(i-1)._2.toString))
        temp_trajecteory+=((trajectory(i)._1,trajectory(i)._2,trajectory(i)._3,trajectory(i)._4,trajectory(i)._5,trajectory(i)._6,trajectory(i)._7))
        if(!temp_station.exists(_._1==trajectory(i)._1)){
          temp_station+=((trajectory(i)._1,trajectory(i)._2,trajectory(i)._3))
        }
        if(i==trajectory.length-1){
          if(temp_trajecteory.length!=temp_station.length){
            var temp_score=compute_score(temp_trajecteory.toArray,temp_station.toArray).sortBy(x=>x._4)(Ordering.Double.reverse)
            var station=temp_score(0)
            val l:Int=temp_trajecteory.length
            result_trajectory+=((station._1,station._2,station._3,temp_trajecteory(0)._4,temp_trajecteory(0)._5,temp_trajecteory(l-1)._6,get_timediff(temp_trajecteory(0)._5.toString,temp_trajecteory(l-1)._6).toString))
            temp_station.clear()
            temp_trajecteory.clear()
          }else{
            //println(temp_trajecteory.length)
            result_trajectory++=(temp_trajecteory)
            temp_station.clear()
            temp_trajecteory.clear()
          }
        }
      }else{
        //        if(temp_trajecteory.length==1){
        //          println(temp_trajecteory.length)
        //          result_trajectory++=(temp_trajecteory)
        //          temp_trajecteory.clear()
        //          temp_station.clear()
        //          temp_trajecteory+=((trajectory(i)._1,trajectory(i)._2,trajectory(i)._3,trajectory(i)._4,trajectory(i)._5,trajectory(i)._6,trajectory(i)._7))
        //          temp_station+=((trajectory(i)._1,trajectory(i)._2,trajectory(i)._3))
        //        }else
        if(temp_trajecteory.length!=temp_station.length){
          var temp_score=compute_score(temp_trajecteory.toArray,temp_station.toArray).sortBy(x=>x._4)(Ordering.Double.reverse)
          var station=temp_score(0)
          val l:Int=temp_trajecteory.length
          result_trajectory+=((station._1,station._2,station._3,temp_trajecteory(0)._4,temp_trajecteory(0)._5,temp_trajecteory(l-1)._6,get_timediff(temp_trajecteory(0)._5.toString,temp_trajecteory(l-1)._6).toString))
          temp_station.clear()
          temp_trajecteory.clear()
          temp_trajecteory+=((trajectory(i)._1,trajectory(i)._2,trajectory(i)._3,trajectory(i)._4,trajectory(i)._5,trajectory(i)._6,trajectory(i)._7))
          temp_station+=((trajectory(i)._1,trajectory(i)._2,trajectory(i)._3))
        }else{
          //println(temp_trajecteory.length)
          result_trajectory++=(temp_trajecteory)
          temp_station.clear()
          temp_trajecteory.clear()
          temp_trajecteory+=((trajectory(i)._1,trajectory(i)._2,trajectory(i)._3,trajectory(i)._4,trajectory(i)._5,trajectory(i)._6,trajectory(i)._7))
          temp_station+=((trajectory(i)._1,trajectory(i)._2,trajectory(i)._3))
        }
        if(i==trajectory.length-1){
          result_trajectory++=(temp_trajecteory)
        }
      }

    }
    result_trajectory.toArray
  }
  //（3，开始时间，触发条件，用户ID，基站编号，基站名称，覆盖范围，经度，纬度，结束时间，停留时间，距离上一个基站的距离，到达该基站的速度）
  //(cid，经度，纬度，用户，起始时间，截止时间，停留时间）
//  def trajectory_merge(trajectory:Array[(String,String,String,String,String,String,String)]):Array[(String,String,String,String,String,String,String)]={
//    var merge_tra=ArrayBuffer[(String,String,String,String,String,String,String)]()
//    var base_station=trajectory(0)._1
//    var start_point:Int=0
//    var end_point:Int=1
//    for (i<-0 until trajectory.length){
//      if((trajectory(i)._1!=base_station) && (i!=(trajectory.length-1))){
//        end_point=i
//        //print(i)
//        base_station=trajectory(i)._1
//        var end_time=trajectory(end_point-1)._6
//        var diff_time_2=get_timediff(trajectory(start_point)._5.toString,trajectory(end_point-1)._6.toString)
//        merge_tra+=((trajectory(start_point)._1,trajectory(start_point)._2,trajectory(start_point)._3,trajectory(start_point)._4,trajectory(start_point)._5,end_time.toString,diff_time_2.toString))
//        start_point=i
//        end_point=i+1
//      }else if((trajectory(i)._1!=base_station) && (i==(trajectory.length-1))){
//        end_point=i
//        //print(i)
//        base_station=trajectory(i)._1
//        var end_time=trajectory(end_point-1)._6
//        var diff_time_2=get_timediff(trajectory(start_point)._5.toString,trajectory(end_point-1)._6.toString)
//        merge_tra+=((trajectory(start_point)._1,trajectory(start_point)._2,trajectory(start_point)._3,trajectory(start_point)._4,trajectory(start_point)._5,end_time.toString,diff_time_2.toString))
//        merge_tra+=((trajectory(i)._1,trajectory(i)._2,trajectory(i)._3,trajectory(i)._4,trajectory(i)._5,trajectory(i)._6,trajectory(i)._7))
//      }else {
//        if (i==(trajectory.length-1)){
//          end_point=i
//          //print(i)
//          var end_time=trajectory(end_point)._6
//          var diff_time_2=get_timediff(trajectory(start_point)._5.toString,trajectory(end_point)._6.toString)
//          merge_tra+=((trajectory(start_point)._1,trajectory(start_point)._2,trajectory(start_point)._3,trajectory(start_point)._4,trajectory(start_point)._5,end_time.toString,diff_time_2.toString))
//        }
//      }
//    }
//    merge_tra.toArray
//  }
  def get_residencePoint(trajectory: Array[(String, String, String, String, String, String, String)]): Array[(String, String, String, String, String, String, String, String)] = {
    val residencePoint = ArrayBuffer[(String, String, String, String, String, String, String, String)]()
    val temp_subtrajectory = ArrayBuffer[(String, String, String, String, String, String, String)]()
    var num_residencePoint: Int = 1
    temp_subtrajectory += ((trajectory(0)._1, trajectory(0)._2, trajectory(0)._3, trajectory(0)._4, trajectory(0)._5, trajectory(0)._6, trajectory(0)._7))
    for (i <- 1 to trajectory.length-1) {
      if (get_distance(trajectory(i)._2, trajectory(i)._3, trajectory(i - 1)._2, trajectory(i - 1)._3) < 0.5) {
        temp_subtrajectory += ((trajectory(i)._1, trajectory(i)._2, trajectory(i)._3, trajectory(i)._4, trajectory(i)._5, trajectory(i)._6, trajectory(i)._7))
        if (i == trajectory.length - 1) {
          if(get_timediff(temp_subtrajectory(0)._5.toString, temp_subtrajectory(temp_subtrajectory.length - 1)._6) > 3600){
            for (k <- 0 to temp_subtrajectory.length-1) {
              residencePoint += ((temp_subtrajectory(k)._1, temp_subtrajectory(k)._2, temp_subtrajectory(k)._3, temp_subtrajectory(k)._4, temp_subtrajectory(k)._5, temp_subtrajectory(k)._6, temp_subtrajectory(k)._7, num_residencePoint.toString))
            }
          }else{
            for (k <- 0 to temp_subtrajectory.length-1) {
              residencePoint += ((temp_subtrajectory(k)._1, temp_subtrajectory(k)._2, temp_subtrajectory(k)._3, temp_subtrajectory(k)._4, temp_subtrajectory(k)._5, temp_subtrajectory(k)._6, temp_subtrajectory(k)._7, "-1"))
            }
          }
          temp_subtrajectory.clear()
        }
      } else if ((get_timediff(temp_subtrajectory(0)._5.toString, temp_subtrajectory(temp_subtrajectory.length - 1)._6) > 3600)) {
        for (k <- 0 to temp_subtrajectory.length-1) {
          residencePoint += ((temp_subtrajectory(k)._1, temp_subtrajectory(k)._2, temp_subtrajectory(k)._3, temp_subtrajectory(k)._4, temp_subtrajectory(k)._5, temp_subtrajectory(k)._6, temp_subtrajectory(k)._7, num_residencePoint.toString))
        }
        temp_subtrajectory.clear()
        temp_subtrajectory += ((trajectory(i)._1, trajectory(i)._2, trajectory(i)._3, trajectory(i)._4, trajectory(i)._5, trajectory(i)._6, trajectory(i)._7))
        num_residencePoint = num_residencePoint + 1
        if (i == trajectory.length - 1) {
          if (get_timediff(temp_subtrajectory(0)._5.toString, temp_subtrajectory(0)._6) > 3600) {
            residencePoint += ((temp_subtrajectory(0)._1, temp_subtrajectory(0)._2, temp_subtrajectory(0)._3, temp_subtrajectory(0)._4, temp_subtrajectory(0)._5, temp_subtrajectory(0)._6, temp_subtrajectory(0)._7, num_residencePoint.toString))
          }
          else {
            residencePoint += ((temp_subtrajectory(0)._1, temp_subtrajectory(0)._2, temp_subtrajectory(0)._3, temp_subtrajectory(0)._4, temp_subtrajectory(0)._5, temp_subtrajectory(0)._6, temp_subtrajectory(0)._7, "-1"))
          }
        }
      } else {
        for (k <- 0 to temp_subtrajectory.length-1) {
          residencePoint += ((temp_subtrajectory(k)._1, temp_subtrajectory(k)._2, temp_subtrajectory(k)._3, temp_subtrajectory(k)._4, temp_subtrajectory(k)._5, temp_subtrajectory(k)._6, temp_subtrajectory(k)._7, "-1"))
        }
        temp_subtrajectory.clear()
        temp_subtrajectory += ((trajectory(i)._1, trajectory(i)._2, trajectory(i)._3, trajectory(i)._4, trajectory(i)._5, trajectory(i)._6, trajectory(i)._7))
        if (i == trajectory.length - 1) {
          if (get_timediff(temp_subtrajectory(0)._5.toString, temp_subtrajectory(0)._6) > 3600) {
            residencePoint += ((temp_subtrajectory(0)._1, temp_subtrajectory(0)._2, temp_subtrajectory(0)._3, temp_subtrajectory(0)._4, temp_subtrajectory(0)._5, temp_subtrajectory(0)._6, temp_subtrajectory(0)._7, num_residencePoint.toString))
          }
          else {
            residencePoint += ((temp_subtrajectory(0)._1, temp_subtrajectory(0)._2, temp_subtrajectory(0)._3, temp_subtrajectory(0)._4, temp_subtrajectory(0)._5, temp_subtrajectory(0)._6, temp_subtrajectory(0)._7, "-1"))
          }
        }
      }
    }
    return residencePoint.toArray
  }
  def trajectory_merge(trajectory: Array[(String, String, String, String, String, String, String)]): Array[(String, String, String, String, String, String, String,String)] = {
    var merge_tra = ArrayBuffer[(String, String, String, String, String, String, String,String)]()
    var base_station = trajectory(0)._1
    var start_point: Int = 0
    var end_point: Int = 1
    for (i <- 1 until trajectory.length) {
      if ((trajectory(i)._1 != base_station||get_timediff(trajectory(i-1)._6,trajectory(i)._5)>10800) && (i != (trajectory.length - 1))) {
        end_point = i
        //print(i)
        base_station = trajectory(i)._1
        var end_time = trajectory(end_point - 1)._6
        var diff_time_2 = get_timediff(trajectory(start_point)._5.toString, trajectory(end_point - 1)._6.toString)
        merge_tra += ((trajectory(start_point)._1, trajectory(start_point)._2, trajectory(start_point)._3, trajectory(start_point)._4, trajectory(start_point)._5, end_time.toString, diff_time_2.toString,(end_point-start_point).toString))
        start_point = i
        end_point = i + 1
      } else if ((trajectory(i)._1 != base_station||get_timediff(trajectory(i-1)._6,trajectory(i)._5)>10800) && (i == (trajectory.length - 1))) {
        end_point = i
        //print(i)
        base_station = trajectory(i)._1
        var end_time = trajectory(end_point - 1)._6
        var diff_time_2 = get_timediff(trajectory(start_point)._5.toString, trajectory(end_point - 1)._6.toString)
        merge_tra += ((trajectory(start_point)._1, trajectory(start_point)._2, trajectory(start_point)._3, trajectory(start_point)._4, trajectory(start_point)._5, end_time.toString, diff_time_2.toString,(end_point-start_point).toString))
        merge_tra += ((trajectory(i)._1, trajectory(i)._2, trajectory(i)._3, trajectory(i)._4, trajectory(i)._5, trajectory(i)._6, trajectory(i)._7,"1"))
      } else {
        if (i == (trajectory.length - 1)) {
          end_point = i
          //print(i)
          var end_time = trajectory(end_point)._6
          var diff_time_2 = get_timediff(trajectory(start_point)._5.toString, trajectory(end_point)._6.toString)
          merge_tra += ((trajectory(start_point)._1, trajectory(start_point)._2, trajectory(start_point)._3, trajectory(start_point)._4, trajectory(start_point)._5, end_time.toString, diff_time_2.toString,(end_point-start_point).toString))
        }
      }
    }
    merge_tra.toArray
  }
}
