//成环区域合并

//元组转字符串重载
def tupleToStr2(tup:(String,String,String,String,String,String,String)):String ={
    var rs = ""
    tup.productIterator.foreach(v=> rs = rs + v.toString+",")
    rs.substring(0,rs.length-1)
}


import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.control._

//测试用
//val stay_record = sc.textFile("/user/student/fy/stay_circle_test.csv")             //(基站编号，经度，纬度，用户，起始时间，截止时间，停留时长，记录个数)
//所有用户记录
//val stay_record = sc.textFile("/user/student/fy/social_user_record.csv").map(x => x+",1")          //没有合并次数的记录文件有次数之后就可以去掉这个map
val stay_record = sc.textFile("/user/student/fy/social_user_record_filter_combine.csv")             //有合并次数的记录文件


val stay_record_group = stay_record.map(_.split(",")).filter(x => x.length == 8).map(x => (x(3),(x(0),x(1),x(2),x(4),x(5),x(6),x(7)))).groupByKey()  //637个用户

def stay_circle_combine(record: Iterable[(String,String,String,String,String,String,String)]): Iterable[String] = {

    val result = new ArrayBuffer[String]();  
    val loop = new Breaks;

    val record_list = record.toList                            //(基站编号，经度，纬度，起始时间，截止时间，停留时长，记录个数)
    //分段，以一小时为时间阈值切割，每段内分析成环情况
    //如果段内记录数小于3，则不分析直接写入
    
    val cut_time = 3600                                        //分段切割时间阈值，暂定一小时
    
    val record_seg = ArrayBuffer[ArrayBuffer[(String,String,String,String,String,String,String)]]()             //存放所有段的数组缓冲，里面的每一个数组缓冲是一个段
    
    var temp_start = 0                                                     //每段起始指针
    var temp_end = 0                                                       //每段终止指针

    var j = 0
    for ( j <- 1 until record_list.length){                                //将全部记录分段
        if ((formatDate(record_list(j)._4)- formatDate(record_list(j-1)._5)) < cut_time){               //如果j对应记录起始时间与上一条记录截止时间的时间差小于3600
            temp_end = j                                                   //加入同一段，段终止指针+1
        }else{                                                                                            //如果时间差大于3600，切断
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
    
    
    
    for (seg <- record_seg.toArray){
        //result += "seg_begin"
        
        val seg_record = seg.toArray                    //seg_record:Array[(String,String,String,String,String,String,String)]
        
        if (seg_record.length >= 3){                    //如果段内记录数大于等于3，则进行成环基站合并分析，如果小于就不分析直接写入结果
            //result += ("seg_length ="+seg_record.length)
        
            var cluster_begin = 0                         //环起始指针，初始化为第一个
            var cluster_end = 2                           //环终止指针，初始化为第三个
                    
            while (cluster_end < seg_record.length){
                                    
                if (seg_record(cluster_begin)._1 == seg_record(cluster_end)._1){     //如果存在环
                    //result += "circle_begin"
                
                    var cid_time_map:Map[String,Int] = Map()                         //环内基站出现次数经纬度字典   “11231,110.3409157,20.03196483, 2”
                    
                    var i = 0
                    for (i <- cluster_begin to cluster_end){
                        var last_time = cid_time_map.get(seg_record(i)._1+","+seg_record(i)._2+","+seg_record(i)._3).getOrElse(0).toInt            //如果字典里已经存在基站，就获取原来的检测数目last_time，如果没有则为0
                        cid_time_map += (seg_record(i)._1+","+seg_record(i)._2+","+seg_record(i)._3 -> (last_time + seg_record(i)._7.toInt) )      //将新一条记录的基站检测记录数目加上原来的值写入字典
                    }
                    
                    var temp_end = cluster_end+1
                    loop.breakable {
                        while (temp_end < seg_record.length){
                            if (cid_time_map.contains(seg_record(temp_end)._1+","+seg_record(temp_end)._2+","+seg_record(temp_end)._3)){                                
                                var j = 0
                                for (j <- cluster_end+1 to temp_end){
                                    var last_time = cid_time_map.get(seg_record(j)._1+","+seg_record(j)._2+","+seg_record(j)._3).getOrElse(0).toInt            //如果字典里已经存在基站，就获取原来的检测数目last_time，如果没有则为0
                                    cid_time_map += (seg_record(j)._1+","+seg_record(j)._2+","+seg_record(j)._3 -> (last_time + seg_record(j)._7.toInt))       //将新一条记录的基站检测记录数目加上原来的值写入字典       
                                }
                                cluster_end = temp_end
                            }
                            
                            if (temp_end - cluster_end == 2){                //成环扩张条件：接下来记录中存在环内基站，最多扩展到next2
                                loop.break;                         
                            }else{
                                temp_end += 1
                            }
                        }
                    }
                    
                    //写入
                    val cid_loc_list = cid_time_map.keys.toList
                    val cid_str = cid_loc_list.map(x => x.split(",")(0)).mkString("\t")
                    val loc_str_lon = cid_loc_list.map(x => x.split(",")(1)).mkString("\t")
                    val loc_str_lat = cid_loc_list.map(x => x.split(",")(2)).mkString("\t")
                    val cid_time_str = cid_time_map.values.toList.mkString("\t")
                    
                    
                    val begin_time = seg_record(cluster_begin)._4
                    val end_time = seg_record(cluster_end)._5
                    val stay_time = (formatDate(seg_record(cluster_end)._5) - formatDate(seg_record(cluster_begin)._4)).toString()
                    
                    // 基站名称列表，基站经度列表，基站纬度列表，起始时间，截止时间，停留时间，基站检测次数列表
                    result += cid_str+ "," + loc_str_lon+ "," +loc_str_lat+"," +begin_time+"," +end_time+"," +stay_time+"," +cid_time_str
                    //更新cluster_begin/end
                    
                    cluster_begin = cluster_end+1
                    cluster_end = cluster_begin+2 
                    //result += "circle_end"
                    //result += ("new_begin = "+ cluster_begin)
                    //result += ("new_end = "+ cluster_end)                                        

                }else{                  
                    //如果不存在环，指针后移，并将移出记录写入，移入记录不写入
/*                     if (cluster_end - cluster_begin == 2){
                        cluster_end += 1
                    }else{
                        result += "non_circle_begin+1"
                        result += tupleToStr2(seg_record(cluster_begin))
                        cluster_begin += 1
                    } */   
                    
                    //更改成环约束条件：如果不存在环，时间窗直接平移，不扩展
                    
                    //result += "non_circle_begin+1"
                    result += tupleToStr2(seg_record(cluster_begin))
                    cluster_begin += 1
                    cluster_end += 1
                    
                }
            }
            //段最后写入
            //如果段内剩下记录不足则写入
            if (cluster_begin < seg_record.length && cluster_end >= seg_record.length){
                var k = 0
                for (k <- cluster_begin until seg_record.length){
                    //result += "seg_left"
                    result += tupleToStr2(seg_record(k))
                }   
            }
            
        }else{
            var l = 0
            for (l <- 0 to seg_record.length-1){
                //result += "seg_length<3"
                result += tupleToStr2(seg_record(l))
                //result += "seg_length<3_end"
            }
        }
        
        //result += "seg_end"
    
    }

    result.toList

}


val stay_record_group_combine = stay_record_group.mapValues(stay_circle_combine(_))
