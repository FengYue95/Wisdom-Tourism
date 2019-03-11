//�û�meeting���

//�ڴ�ŵ��£�������취����

//���ҵ�ʱ������RDD�в���,������Ūһ��չ����RDD���������ļ�stay_record_group_combine_flat
import java.io.FileWriter
val out = new FileWriter("/home/student/fy/stay_record_group_combine_flat.csv")      //�ļ������ڷ�����������hdfs
for (t <- stay_record_group_combine.collect()){
    //out.write(t._1+"\r\n")   //�û�
    val values = t._2
    val value = ""
    for (value <- t._2){
        out.write(t._1+","+value+"\r\n")
    }
}
out.close()






//������ ���ձ���û�ɸѡ��¼
//haikou_node292Ϊ���ڲ������罻��ϵ��292���û��б����Ǵ�stay_record_group_combine�����û���¼���ҳ���Щ�˵ļ�¼test_user_record
val test_user = sc.textFile("/user/student/fy/haikou_node292.csv").map(x =>(x,""))    //292���˼�¼
val test_user_record = test_user.leftOuterJoin(stay_record_group_combine).filter(x => x._2._2 != None).map(x => (x._1,x._2._2.get))

//չ��RDDtest_user_record�����ڲ���
/* import java.io.FileWriter
val out = new FileWriter("/home/student/fy/haikou_node292_flat.csv")      //�ļ������ڷ�����������hdfs
for (t <- test_user_record.collect()){
    //out.write(t._1+"\r\n")   //�û�
    val values = t._2
    val value = ""
    for (value <- t._2){
        out.write(t._1+","+value+"\r\n")
    }
}
out.close() */

//Ȼ���ϴ���hdfs���ٶ�ȡ
val test_user_record_flat = sc.textFile("/user/student/fy/haikou_node292_flat.csv") 


//val stay_record_group_combine_flat = sc.textFile("/user/student/fy/stay_record_group_combine_flat.csv")  
//�û�����վ�����б���վ�����б���վγ���б���ʼʱ�䣬��ֹʱ�䣬ͣ��ʱ�䣬��վ�������б�


var user_edge_map:Map[String,Array[String]] = Map()    //�û���array((�ڽӽ��1��Ȩ��1),(�ڽӽ��2��Ȩ��2),...,)
//all_edge���ڱ������б���Ϣ
var all_edge = new ArrayBuffer[String]()

//��ָ��������exp(-1/gamma*x),gamma�����½��ٶ�
//Ϊ��ʹ˥����Ҫ̫�죬�����������Ҫ
//���ɳ�������˥�������Ա�Ч��
def func(x:Double):Double={
    val gamma = 450
    Math.exp(-x/gamma)
}

def func2(x:Double):Double={
    val gamma2 = 1850*3
    Math.exp(-x/gamma2)
}




//������
for (t <- test_user_record.collect().par){

//������ 1���û�
//for (t <- stay_record_group_combine.take(1)){
//�����û�
//for (t <- stay_record_group_combine.collect()){
    var user = t._1           //�û�
    var records = t._2.toList         //�û����м�¼records
    var edge_arr = new ArrayBuffer[String]()   //��������û��������ڽӽ�㼰��Ȩ��
    
    var window_begin = formatDate(records(0).split(",")(3)) - 900                   //ʱ�䴰Ϊ��׼�û���¼ǰ����չ900s
    var window_end = formatDate(records(0).split(",")(4)) + 900
    
    var i = 0
    for (i <- 0 until records.length){      //records(i):�û�ÿһ����¼  (��վ�����б���վ�����б���վγ���б���ʼʱ�䣬��ֹʱ�䣬ͣ��ʱ�䣬��վ�������б�)
    
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

        
        //�������¼�¼��ϵ��ȷ��ʱ�䴰����
        window_end = record_end+900        //���������ʱ�䴰��ָֹ��
        if (i+1 < records.length){
            var record_time_diff  = formatDate(records(i+1).split(",")(3)) - record_end
            if (record_time_diff  < 1800){
                window_end = (formatDate(records(i+1).split(",")(3)) + record_end)/2    //���������¼��������ָֹ��ȡ������¼�е�
            }
        }
        
        //��������¼�����ٱߵ����нڵ��ҳ�����������ó����ٱ��û���Ȩ��д���û��ٱ�����edge_arr
        //������
        val meeting_list = test_user_record_flat.map(_.split(",")).filter(x => x(0) != user).filter(x => (formatDate(x(4)) > window_begin && formatDate(x(4)) < window_end) || (formatDate(x(5)) > window_begin && formatDate(x(5)) < window_end) || (formatDate(x(4)) < window_begin && formatDate(x(5)) > window_end)).filter(x => (x(1).split("\t").toSet).&(cid_list.toSet).size != 0).collect()
        //val meeting_list = stay_record_group_combine_flat.map(_.split(",")).filter(x => x(0) != user).filter(x => (formatDate(x(4)) > window_begin && formatDate(x(4)) < window_end) || (formatDate(x(5)) > window_begin && formatDate(x(5)) < window_end) || (formatDate(x(4)) < window_begin && formatDate(x(5)) > window_end)).filter(x => (x(1).split("\t").toSet).&(cid_list.toSet).size != 0).collect()
        
        val p = 0
        for (p <- 0 until meeting_list.length){
            //meeting�ص��ص�Ȩ��weight_loc����
            var weight_loc = 0.0                      
            
            val meet_user = meeting_list(p)(0)
            val meet_cid_list = meeting_list(p)(1).split("\t")
            val meet_begin = formatDate(meeting_list(p)(4)) //long
            val meet_end = formatDate(meeting_list(p)(5))   //long
            val meet_num_list = meeting_list(p)(7).split("\t")
            
            var meet_cid_num_sum = 0.0                //��⵽�Ĵ����ĺ�
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
                weight_loc += Math.min(center_weight_loc,edge_weight_loc)                     //meeting�ص��ص�Ȩ�ؼ���
            }
            
            //meetingʱ���ص�Ȩ��weight_time����
            var weight_time = 1.0
            if (meet_end < record_begin){                //ʱ��û���ص����������,weight_time��������¼�ⲿ���������ָ��˥��
                weight_time *= func((record_begin - meet_end).toDouble)
            }else if (meet_begin > record_end){
                weight_time *= func((meet_begin - record_end).toDouble)
            }else{                                       //ʱ�����ص�ʱ��weight_time����time_overlap���׼��¼���ȱ�ֵ
                val time_overlap = (Math.min(record_end,meet_end)-Math.max(record_begin,meet_begin)).toDouble
                val time_union = (Math.max(record_end,meet_end)-Math.min(record_begin,meet_begin)).toDouble
                if (record_end != record_begin){         //����׼�û���¼ͣ��ʱ�䲻Ϊ��ʱ
                    //weight_time += time_overlap/(record_end-record_begin)    //�ص��ȶ���1�������û�ʱ�䴰����
                    weight_time += time_overlap/time_union    //�ص��ȶ���2�����Բ���ʱ�䴰����
                }
            }
            val record_len = record_end - record_begin
            val meet_len = meet_end - meet_begin
            if (Math.abs(record_len-meet_len) > 1850){
                if (record_len == 0 || meet_len == 0){                  //����м�¼����Ϊ0����ôֻ�жϲ�����1850��ɾ��
                    weight_time = 0
                }else if (record_len / meet_len > 3 || meet_len / record_len > 3){      //û�м�¼����Ϊ0��������,shang
                    weight_time = 0
                }                                                  
            }
            
            //��¼ͣ��ʱ���ڲ�����˥��
            //weight_time *= func2((Math.abs(record_len-meet_len)).toDouble)
            
            //meeting�����weight_POIȨ�ؼ���
            val weight_POI = 1.0
            //��Ȩ��
            val weight_all = weight_loc*weight_time*weight_POI
            
            if (weight_all != 0){
                edge_arr += (user+","+meet_user+","+weight_loc+","+weight_time+","+weight_all)
                //edge_arr += (records(i)+"\t"+meeting_list(p).mkString(",")+"\t"+weight_loc+","+weight_time)
            }
 
        }
        //����window_begin
        window_begin = window_end
        
    }
    //�����βʱ������һ���û����ٱ߱�д���ܱ�user_edge_map
    user_edge_map += (user -> edge_arr.toArray) 
    val temp_array = edge_arr.toArray
    all_edge ++= temp_array
}

val all_edge_array = all_edge.toArray
val all_edge_rdd = sc.parallelize(all_edge_array)

all_edge_rdd.repartition(1).saveAsTextFile("/user/student/fy/all_edge_rdd_collect_par")
