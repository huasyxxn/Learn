package com.huangys.flink.content

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.huangys.flink.util.UDF
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.json.JSONObject
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import redis.clients.jedis.{HostAndPort, JedisCluster}


class UcmsHandle {
  def process(ucmsDataStream:DataStream[String],env:StreamExecutionEnvironment): Unit ={
    val ucmsArticleStream = ucmsDataStream
      .map(new Data2UcmsArticle)



    //ucms非自媒体发文
    val ucmsInfoStream = ucmsArticleStream.filter(data=>data.id.startsWith("ucms") && !data.editor_mail.equals("weMedia"))
    //ucms自媒体发文
    val ucmsZmtInfoStreamById = ucmsArticleStream.filter(data=>data.id.startsWith("ucms")
      && data.editor_mail.equals("weMedia"))
    val ucmsZmtInfoStreamByFhh = ucmsArticleStream.filter(data=>data.id.startsWith("ucms")
      && data.editor_mail.equals("weMedia")
      && data.fhh.startsWith("sub"))
      .map(data=>{
        UcmsArticle(data.fhh:String,data.url_pc:String,data.title:String,data.src:String,data.tm:String,data.editor_mail:String,
          data.url_iifeng:String,data.is_original:Int,data.original:Int,data.original_wemedia:Int,
          data.zmt:String,data.fhh:String,data.is_original_new:String,data.joinId:String,data.pt:String,data.copyright:String,
          data.sourceFrom:String,data.dataSource:Int,data.tsrc:String,data.source_link:String,data.num:Int,data.word_num:Int,
          data.dataProvider:Int,data.tm2:String,data.updater:String,data.accountType:Int,data.level:Int,data.accId:String,data.fhtId,
          data.applyTime,data.online:Int,data.fhhcopyright,data.cr:String,data.contract,data.acco,data.issuetype)
      })
    val ucmsZmtInfo = ucmsZmtInfoStreamById union ucmsZmtInfoStreamByFhh

    val ucmsStream = (ucmsInfoStream
      .filter(data => !data.pt.equals("video"))
      .map(data=>getUcmsType(data)) union
      ucmsZmtInfo.map(data=>getUcmsZmtType(data))).map(data=>{
      data.toString
    })


  }

  /**
    * 自媒体Ucms数据处理
    */
  def getUcmsZmtType(data:UcmsArticle): UcmsArticle ={
    var typ = "#"
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    if(data.fhtId != null){
      if(data.dataSource == 1
      ||data.dataSource == 9
      ||data.dataSource == 16
      ||data.dataProvider == 2){
        typ = "dfh_reg_art"
      }else{
        typ = "dfh_spider_art"
      }
    }else if(data.accountType == 4
      && data.applyTime >= sdf.parse("2017-07-01 00:00:00").getTime
      && data.fhhcopyright == 1){
      typ = "yidian_art"
    }else if (
      (
        (
          data.accountType == 4 &&
            (
              data.applyTime < sdf.parse("2017-07-01 00:00:00").getTime ||
                (
                  data.applyTime >= sdf.parse("2017-07-01 00:00:00").getTime &&
                    (
                      data.fhhcopyright != 1 || data.fhhcopyright == null
                    )
                )
            )
          ) || (
          data.accountType == 1 && data.dataSource == 11
          )
        ) && !data.contract && !data.acco
    ){
      typ = "spider_yidian_art"
    }else if (data.contract){
      typ = "con_contract_spider_art"
    }else if (data.accountType == 2){
      if(data.dataSource == 1
        ||data.dataSource == 9
        ||data.dataSource == 16
        ||data.dataProvider == 2){
        typ = "con_appo_hand_art"
      }else{
        typ = "con_appo_spider_art"
      }
    }else if (data.acco){
      typ = "con_acco_spider_art"
    }else if (data.dataSource == 12 || data.dataSource == 20004){
      typ = "other_art"
    }else {
      typ = "unknown_art"
    }
    data.issuetype = typ
    data
  }

  /**
    * 非自媒体ucms数据处理
    */
  def getUcmsType(data:UcmsArticle): UcmsArticle ={
    var typ = "#"
    if(data.contract){
      if(data.editor_mail.equals("robot") || data.editor_mail.equals("spider")){
        typ = "con_contract_spider_art"
      }else{
        typ = "con_contract_hand_art"
      }
    }else if(data.acco){
      if(data.editor_mail.equals("robot") || data.editor_mail.equals("spider")){
        typ = "con_acco_spider_art"
      }else{
        typ = "con_acco_hand_art"
      }
    }else{
      if(data.editor_mail.equals("robot") || data.editor_mail.equals("spider")){
        typ = "spider_wct_art"
      }else{
        typ = "other_art"
      }
    }
    data.issuetype = typ
    data
  }
}
case class UcmsArticle(id:String,url_pc:String,title:String,src:String,tm:String,editor_mail:String,
                       url_iifeng:String,is_original:Integer,original:Integer,original_wemedia:Integer,
                       zmt:String,fhh:String,is_original_new:String,joinId:String,pt:String,copyright:String,
                       sourceFrom:String,dataSource:Integer,tsrc:String,source_link:String,num:Integer,word_num:Integer,
                       dataProvider:Integer,tm2:String,updater:String,accountType:Integer,level:Integer,accId:String,
                       fhtId:Integer,applyTime:java.lang.Long,online:Integer,fhhcopyright:Integer,cr:String,contract:Boolean,
                       acco:Boolean,var issuetype:String)



class Data2UcmsArticle extends RichMapFunction[String,UcmsArticle]{
  var jedisCluster:JedisCluster = _
  override def open(parameters: Configuration): Unit = {
    val nodes = new util.HashSet[HostAndPort]()
    nodes.add(new HostAndPort("10.80.28.154",6379))
    nodes.add(new HostAndPort("10.80.29.154",6379))
    nodes.add(new HostAndPort("10.80.30.154",6379))
    nodes.add(new HostAndPort("10.80.31.154",6379))
    nodes.add(new HostAndPort("10.80.32.154",6379))
    jedisCluster = new JedisCluster(nodes)
  }

  override def close(): Unit = {
    jedisCluster.close()
  }

  /**
    *
    * @param ucmsData ucms发文数据
    * @return 结构化数据
    */
  override def map(ucmsData: String): UcmsArticle = {
    val ucmsJson = new JSONObject(ucmsData)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val udf = new UDF
    val accountInfo:String = jedisCluster.get("account_"+getJsonString(ucmsJson,"zmt"))
    val accountJson = new JSONObject(if (accountInfo != null) accountInfo else "{}")
    UcmsArticle(
      "ucms_"+getJsonString(ucmsJson,"id"),
      getJsonString(ucmsJson,"url").replace("https","http"),
      getJsonString(ucmsJson,"title").replaceAll("\\t|\\n",""),
      udf.smh(getJsonString(ucmsJson,"source")),
      sdf.format(new Date(getJsonInteger(ucmsJson,"timestamp").longValue())),
      getJsonString(ucmsJson,"creator"),
      getJsonString(ucmsJson,"wapUrl").replace("https","http"),
      if(getJsonInteger(ucmsJson,"is_original")==2 || getJsonInteger(ucmsJson,"Is_original_wemedia")==1) 1
      else getJsonInteger(ucmsJson,"is_original"),
      getJsonInteger(ucmsJson,"is_original"),
      getJsonInteger(ucmsJson,"Is_original_wemedia"),
      getJsonString(ucmsJson,"zmt"),
      udf.ucmsPrefix(
        if (getJsonString(ucmsJson,"fhh") == null) "#"
        else if (getJsonString(ucmsJson,"fhh").length == 11) "ucms_"+getJsonString(ucmsJson,"fhh")
        else "sub_"+getJsonString(ucmsJson,"fhh")
      ),
      "#",
      "ucms_"+getJsonString(ucmsJson,"id"),
      "ucms_"+getJsonString(ucmsJson,"type"),
      "#",
      "#",
      getJsonInteger(ucmsJson,"dataSource"),
      udf.smh(getJsonString(ucmsJson,"source")),
      "#",
      getJsonInteger(ucmsJson,"page"),
      getJsonInteger(ucmsJson,"word_num"),
      getJsonInteger(ucmsJson,"dataProvider"),
      getJsonString(ucmsJson,"tm2"),
      getJsonString(ucmsJson,"updater"),
      getJsonInteger(accountJson,"accountType"),
      getJsonInteger(accountJson,"level"),
      getJsonString(accountJson,"_id"),
      getJsonInteger(accountJson,"fhtId"),
      getJsonLong(accountJson,"applyTime"),
      getJsonInteger(accountJson,"online"),
      getJsonInteger(accountJson,"fhhCopyright"),
      getJsonString(accountJson,"copyright"),
      jedisCluster.exists("contract_"+udf.smh(getJsonString(ucmsJson,"source"))),
      jedisCluster.exists("acco_"+udf.smh(getJsonString(ucmsJson,"source"))),
      "#"
    )
  }
  def getJsonString(ucmsJson:JSONObject,key:String): String ={
    try{
      if(ucmsJson.has(key)){
        ucmsJson.getString(key)
      }else{
        "#"
      }
    }catch {
      case e:Exception=>"#"
    }

  }
  def getJsonInteger(ucmsJson:JSONObject,key:String): Integer ={
    try{
      if(ucmsJson.has(key)){
        ucmsJson.getInt(key)
      }else{
        null
      }
    }catch {
      case e:Exception=>null
    }

  }
  def getJsonLong(ucmsJson:JSONObject,key:String): java.lang.Long ={
    try{
      if(ucmsJson.has(key)){
        ucmsJson.getLong(key)
      }else{
        null
      }
    }catch {
      case e:Exception=>null
    }

  }
}
