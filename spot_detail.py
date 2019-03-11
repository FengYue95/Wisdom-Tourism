#合并后的基站表cid_loc_id_label_all_combine.txt
data = []
text = []
for line in open("cid_loc_id_label_all_combine.txt","r"): #设置文件对象并读取每一行文件
    text.append(line[:-1])
    location = line.split(",")
    location_pro = "{:.6f}".format(float(location[1]))+","+"{:.6f}".format(float(location[0]))
    data.append(location_pro)               #将每一行文件加入到list中

from urllib import parse
import hashlib

from urllib.request import urlopen
import json

pois_dict = set()
# 获取基站经纬度逆编码信息（附近POI）
def get_cid_detail(location):
    
    queryStr = '/geocoder/v2/?location=%s&output=json&pois=1&radius=100&latest_admin=1&ak=yourak'  %location
    # 对queryStr进行转码，safe内的保留字符不转换
    encodedStr = parse.quote(queryStr, safe="/:=&?#+!$,;'@()*[]")
 
    # 在最后直接追加上yoursk
    rawStr = encodedStr + 'yoursk'
 
    #计算sn
    sn = (hashlib.md5(parse.quote_plus(rawStr).encode("utf8")).hexdigest())
    #print(sn)
     
    #由于URL里面含有中文，所以需要用parse.quote进行处理，然后返回最终可调用的url
    url = parse.quote("http://api.map.baidu.com"+queryStr+"&sn="+sn, safe="/:=&?#+!$,;'@()*[]")  
    response = urlopen(url).read().decode('utf-8')
    #将返回的数据转化成json格式
    responseJson = json.loads(response)
    #获取
    city = responseJson.get('result')['addressComponent']['city']
    district = responseJson.get('result')['addressComponent']['district']
    pois = responseJson.get('result')['pois']               #POI数组
    
    poi_all = {}
    
    for poi in pois:
        pois_dict.add(poi['uid'])
        if poi['uid'] not in poi_all:
            poi_detail = poi['tag']+","+poi['name']+","+poi['direction']+","+poi['distance']
            poi_all[poi['uid']]= poi_detail
        
    poiRegions = responseJson.get('result')['poiRegions']   #POI归属区域面数组？
    
    for poiRegion in poiRegions:
        pois_dict.add(poiRegion['uid'])
        if poiRegion['uid'] not in poi_all:
            poi_detail = poiRegion['tag']+","+poiRegion['name']+","+poiRegion['direction']+","+poiRegion['distance']
            poi_all[poiRegion['uid']]= poi_detail
    
    print(url)
    result = city+","+district+","+str(poi_all)
     
    return result
	
# 利用景点uid,获取景点详细信息

def get_uid_detail(uid):
 
    queryStr = '/place/v2/detail?uid=%s&output=json&scope=2&ak=yourak'  %uid
 
    # 对queryStr进行转码，safe内的保留字符不转换
    encodedStr = parse.quote(queryStr, safe="/:=&?#+!$,;'@()*[]")
 
    # 在最后直接追加上yoursk
    rawStr = encodedStr + 'yoursk'
 
    #计算sn
    sn = (hashlib.md5(parse.quote_plus(rawStr).encode("utf8")).hexdigest())
    #print(sn)
     
    #由于URL里面含有中文，所以需要用parse.quote进行处理，然后返回最终可调用的url
    url = parse.quote("http://api.map.baidu.com"+queryStr+"&sn="+sn, safe="/:=&?#+!$,;'@()*[]")  
    #print(url)
    response = urlopen(url).read().decode('utf-8')
    #将返回的数据转化成json格式
    responseJson = json.loads(response)
    #获取一定有的
    lng = responseJson.get('result')['location']['lng']
    lat = responseJson.get('result')['location']['lat']
    name = responseJson.get('result')['name']
    city = responseJson.get('result').get('city',"")
    area = responseJson.get('result').get('area',"")
    tag = responseJson.get('result')['detail_info']['tag']
    
    #可能没有的
    overall_rating = responseJson.get('result')['detail_info'].get('overall_rating',"")
    shop_hours = responseJson.get('result')['detail_info'].get('shop_hours',"")
    price = responseJson.get('result')['detail_info'].get('price',"")
    level = responseJson.get('result')['detail_info'].get('level',"")
    scope_grade = responseJson.get('result')['detail_info'].get('scope_grade',"")
    telephone = responseJson.get('result').get('telephone',"")
    content_tag = responseJson.get('result')['detail_info'].get('content_tag',"")

    result = uid+"\t"+name+"\t"+"{:.6f}".format(lng)+"\t"+"{:.6f}".format(lat)+"\t"+city+"\t"+area+"\t"+tag+"\t"+overall_rating+"\t"+shop_hours+"\t"+price+"\t"+level+"\t"+scope_grade+"\t"+telephone+"\t"+content_tag
    
    return result
	
