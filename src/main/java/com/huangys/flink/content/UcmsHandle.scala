package com.huangys.flink.content

import java.text.SimpleDateFormat
import java.util.Date

import com.huangys.flink.util.UDF
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.json.JSONObject

import scala.collection.mutable

class UcmsHandle {
  /**
    *
    * @param ucmsData ucms发文数据
    * @return 结构化数据
    */
  def Data2UcmsArticle(ucmsData:String): UcmsArticle ={
    val ucmsJson = new JSONObject(ucmsData)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val udf = new UDF
    UcmsArticle(
      "ucms_"+getJsonString(ucmsJson,"id"),
      getJsonString(ucmsJson,"url").replace("https","http"),
      getJsonString(ucmsJson,"title").replaceAll("\\t|\\n",""),
      udf.smh(getJsonString(ucmsJson,"source")),
      sdf.format(new Date(getJsonInteger(ucmsJson,"timestamp"))),
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
      getJsonString(ucmsJson,"updater")
    )
  }
  def getJsonString(ucmsJson:JSONObject,key:String): String ={
    if(ucmsJson.has(key)){
      ucmsJson.getString(key)
    }else{
      "#"
    }
  }
  def getJsonInteger(ucmsJson:JSONObject,key:String): Int ={
    if(ucmsJson.has(key)){
      ucmsJson.getInt(key)
    }else{
      null
    }
  }
  def process(ucmsDataStream:DataStream[String],env:StreamExecutionEnvironment): Unit ={
    val ucmsArticleStream = ucmsDataStream.map(data=>Data2UcmsArticle(data))

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
          data.dataProvider:Int,data.tm2:String,data.updater:String)
      })
    val ucmsZmtInfo = ucmsZmtInfoStreamById union ucmsZmtInfoStreamByFhh
  }
}
case class UcmsArticle(id:String,url_pc:String,title:String,src:String,tm:String,editor_mail:String,
                       url_iifeng:String,is_original:Int,original:Int,original_wemedia:Int,
                       zmt:String,fhh:String,is_original_new:String,joinId:String,pt:String,copyright:String,
                       sourceFrom:String,dataSource:Int,tsrc:String,source_link:String,num:Int,word_num:Int,
                       dataProvider:Int,tm2:String,updater:String)
