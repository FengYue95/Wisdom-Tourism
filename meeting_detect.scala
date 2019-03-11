//用户meeting检测

//内存放得下！可以想办法搞了

//查找的时候还是在RDD中查找,所以先弄一个展开的RDD，保存至文件stay_record_group_combine_flat
import java.io.FileWriter
val out = new FileWriter("/home/student/fy/stay_record_group_combine_flat.csv")      //文件保存在服务器，不是hdfs
for (t <- stay_record_group_combine.collect()){
    //out.write(t._1+"\r\n")   //用户
    val values = t._2
    val value = ""
    for (value <- t._2){
        out.write(t._1+","+value+"\r\n")
    }
}
out.close()






//测试用 按照表格用户筛选记录
//haikou_node292为海口并具有社交关系的292名用户列表，我们从stay_record_group_combine所有用户记录中找出这些人的记录test_user_record
val test_user = sc.textFile("/user/student/fy/haikou_node292.csv").map(x =>(x,""))    //292个人记录
val test_user_record = test_user.leftOuterJoin(stay_record_group_combine).filter(x => x._2._2 != None).map(x => (x._1,x._2._2.get))

//展开RDDtest_user_record，用于查找
/* import java.io.FileWriter
val out = new FileWriter("/home/student/fy/haikou_node292_flat.csv")      //文件保存在服务器，不是hdfs
for (t <- test_user_record.collect()){
    //out.write(t._1+"\r\n")   //用户
    val values = t._2
    val value = ""
    for (value <- t._2){
        out.write(t._1+","+value+"\r\n")
    }
}
out.close() */

//然后上传至hdfs，再读取
val test_user_record_flat = sc.textFile("/user/student/fy/haikou_node292_flat.csv") 


//val stay_record_group_combine_flat = sc.textFile("/user/student/fy/stay_record_group_combine_flat.csv")  
//用户，基站名称列表，基站经度列表，基站纬度列表，起始时间，截止时间，停留时间，基站检测次数列表


var user_edge_map:Map[String,Array[String]] = Map()    //用户：array((邻接结点1，权重1),(邻接结点2，权重2),...,)
//all_edge用于保存所有边信息
var all_edge = new ArrayBuffer[String]()

//负指数函数，exp(-1/gamma*x),gamma调节下降速度
//为了使衰减不要太快，这个参数很重要
//还可尝试其他衰减函数对比效果
def func(x:Double):Double={
    val gamma = 450
    Math.exp(-x/gamma)
}

def func2(x:Double):Double={
    val gamma2 = 1850*3
    Math.exp(-x/gamma2)
}




//测试用
for (t <- test_user_record.collect().par){

//测试用 1个用户
//for (t <- stay_record_group_combine.take(1)){
//所有用户
//for (t <- stay_record_group_combine.collect()){
    var user = t._1           //用户
    var records = t._2.toList         //用户所有记录records
    var edge_arr = new ArrayBuffer[String]()   //包含这个用户的所有邻接结点及边权重
    
    var window_begin = formatDate(records(0).split(",")(3)) - 900                   //时间窗为基准用户记录前后扩展900s
    var window_end = formatDate(records(0).split(",")(4)) + 900
    
    var i = 0
    for (i <- 0 until records.length){      //records(i):用户每一条记录  (基站名称列表，基站经度列表，基站纬度列表，起始时间，截止时间，停留时间，基站检测次数列表)
    
        val record_arr = records(i).split(",")
        val cid_list = record_arr(0).split("\t")
        val record_begin = formatDate(record_arr(3)) //long
        val record_end = formatDate(record_arr(4))   //long
        val cid_num_list = record_arr(6).split("\t")
        var cid_num_sum = 0.0
        var cid = ""
        for (cid <- cid_num_list){
            cid_num_sum += cid.toDouble
        }

        
        //根据上下记录关系，确定时间窗长度
        window_end = record_end+900        //正常情况下时间窗终止指针
        if (i+1 < records.length){
            var record_time_diff  = formatDate(records(i+1).split(",")(3)) - record_end
            if (record_time_diff  < 1800){
                window_end = (formatDate(records(i+1).split(",")(3)) + record_end)/2    //如果两条记录过近，终止指针取两条记录中点
            }
        }
        
        //将这条记录存在临边的所有节点找出，并将计算得出的临边用户及权重写入用户临边数组edge_arr
        //测试用
        val meeting_list = test_user_record_flat.map(_.split(",")).filter(x => x(0) != user).filter(x => (formatDate(x(4)) > window_begin && formatDate(x(4)) < window_end) || (formatDate(x(5)) > window_begin && formatDate(x(5)) < window_end) || (formatDate(x(4)) < window_begin && formatDate(x(5)) > window_end)).filter(x => (x(1).split("\t").toSet).&(cid_list.toSet).size != 0).collect()
        //val meeting_list = stay_record_group_combine_flat.map(_.split(",")).filter(x => x(0) != user).filter(x => (formatDate(x(4)) > window_begin && formatDate(x(4)) < window_end) || (formatDate(x(5)) > window_begin && formatDate(x(5)) < window_end) || (formatDate(x(4)) < window_begin && formatDate(x(5)) > window_end)).filter(x => (x(1).split("\t").toSet).&(cid_list.toSet).size != 0).collect()
        
        val p = 0
        for (p <- 0 until meeting_list.length){
            //meeting地点重叠权重weight_loc计算
            var weight_loc = 0.0                      
            
            val meet_user = meeting_list(p)(0)
            val meet_cid_list = meeting_list(p)(1).split("\t")
            val meet_begin = formatDate(meeting_list(p)(4)) //long
            val meet_end = formatDate(meeting_list(p)(5))   //long
            val meet_num_list = meeting_list(p)(7).split("\t")
            
            var meet_cid_num_sum = 0.0                //检测到的次数的和
            var meet_cid = ""
            for (meet_cid <- meet_num_list){
                meet_cid_num_sum += meet_cid.toDouble
            }
            
            val cid_common_set = (meet_cid_list.toSet).&(cid_list.toSet)
            val loc = ""
            for (loc <- cid_common_set){
                var center_weight_loc = 0.0
                var edge_weight_loc = 0.0
                
                val j = 0
                for (j <- 0 until cid_list.length){
                    if (cid_list(j)==loc){
                        center_weight_loc = cid_num_list(j).toDouble/cid_num_sum
                    }
                }
                val q = 0
                for (q <- 0 until meet_cid_list.length){
                    if (meet_cid_list(q)==loc){
                        edge_weight_loc = meet_num_list(q).toDouble/meet_cid_num_sum
                    }
                }
                weight_loc += Math.min(center_weight_loc,edge_weight_loc)                     //meeting地点重叠权重计算
            }
            
            //meeting时间重叠权重weight_time计算
            var weight_time = 1.0
            if (meet_end < record_begin){                //时间没有重叠的两种情况,weight_time随两条记录外部距离增大而指数衰减
                weight_time *= func((record_begin - meet_end).toDouble)
            }else if (meet_begin > record_end){
                weight_time *= func((meet_begin - record_end).toDouble)
            }else{                                       //时间有重叠时，weight_time加上time_overlap与基准记录长度比值
                val time_overlap = (Math.min(record_end,meet_end)-Math.max(record_begin,meet_begin)).toDouble
                val time_union = (Math.max(record_end,meet_end)-Math.min(record_begin,meet_begin)).toDouble
                if (record_end != record_begin){         //当基准用户记录停留时间不为零时
                    //weight_time += time_overlap/(record_end-record_begin)    //重叠度定义1，除以用户时间窗长度
                    weight_time += time_overlap/time_union    //重叠度定义2，除以并集时间窗长度
                }
            }
            val record_len = record_end - record_begin
            val meet_len = meet_end - meet_begin
            if (Math.abs(record_len-meet_len) > 1850){
                if (record_len == 0 || meet_len == 0){                  //如果有记录长度为0，那么只判断差，差大于1850则删除
                    weight_time = 0
                }else if (record_len / meet_len > 3 || meet_len / record_len > 3){      //没有记录长度为0，则考虑商,shang
                    weight_time = 0
                }                                                  
            }
            
            //记录停留时间内部距离衰减
            //weight_time *= func2((Math.abs(record_len-meet_len)).toDouble)
            
            //meeting活动类型weight_POI权重计算
            val weight_POI = 1.0
            //总权重
            val weight_all = weight_loc*weight_time*weight_POI
            
            if (weight_all != 0){
                edge_arr += (user+","+meet_user+","+weight_loc+","+weight_time+","+weight_all)
                //edge_arr += (records(i)+"\t"+meeting_list(p).mkString(",")+"\t"+weight_loc+","+weight_time)
            }
 
        }
        //更新window_begin
        window_begin = window_end
        
    }
    //到达结尾时，将上一个用户的临边表写入总表user_edge_map
    user_edge_map += (user -> edge_arr.toArray) 
    val temp_array = edge_arr.toArray
    all_edge ++= temp_array
}

val all_edge_array = all_edge.toArray
val all_edge_rdd = sc.parallelize(all_edge_array)

all_edge_rdd.repartition(1).saveAsTextFile("/user/student/fy/all_edge_rdd_collect_par")
