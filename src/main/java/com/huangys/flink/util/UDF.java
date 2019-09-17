package com.huangys.flink.util;


import org.apache.hadoop.io.Text;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UDF implements Serializable {


    public String ucmsPrefix1(String id,boolean t){
        System.out.println(id+"\t"+ucmsPrefix(id));
        return ucmsPrefix(id);
    }
    public String ucmsPrefix(String id){
        if(id.split("_").length==2){
            String eArticleId = id.split("_")[1];
            if(eArticleId.length()==19) {
                try {
                    return "ucms_"+Base62.encode(Long.parseLong(eArticleId));
                }catch (Exception e){

                }
            }else if(eArticleId.length()==11) {
                return "ucms_"+eArticleId;
            }
        }else if(id.split("_").length==1){
            String eArticleId = id;
            if(eArticleId.length()==19) {
                try {
                    return "ucms_"+Base62.encode(Long.parseLong(eArticleId));
                }catch (Exception e){

                }
            }else if(eArticleId.length()==11) {
                return "ucms_"+eArticleId;
            }
        }

        return id;
    }

    public String getChannel(String jsonStr,String key){
        try {
            JSONObject jsonObject = new JSONObject(jsonStr);
            String[] rs = jsonObject.getString(key).replaceAll("\n|\t|\r|\\[|\\]", "").split(",");
            String r = "";
            for(String rr :rs){
                if(rr.equals("1")){
                    if(key.equals("articleChannel"))
                        r=r+"凤凰新闻;";
                    else if (key.equals("videoChannel"))
                        r=r+"凤凰视频;";
                }else if(rr.equals("2")){
                    if(key.equals("articleChannel"))
                        r=r+"凤凰新闻客户端;";
                    else if (key.equals("videoChannel"))
                        r=r+"凤凰视频客户端;";
                }else if(rr.equals("3")){
                    if(key.equals("articleChannel"))
                        r=r+"一点资讯;";
                    else if (key.equals("videoChannel"))
                        r=r+"凤凰视频;";
                }else if(rr.equals("4")){
                    if(key.equals("articleChannel"))
                        r=r+"手机凤凰网;";
                    else if (key.equals("videoChannel"))
                        r=r+"手机凤凰网;";
                }else if(rr.equals("5")){
                    if(key.equals("articleChannel"))
                        r=r+"智能推荐;";
                    else if (key.equals("videoChannel"))
                        r=r+"智能推荐;";
                }
            }
            return r;
        }catch (Exception e){
            //e.printStackTrace();
            return "#" ;
        }
    }


    public String getPar(String parameterString, String separator, String parameterName) {
        try {
            if (parameterString == null || parameterString.toString().equals("#") || parameterString.toString().equals("")) {
                return "#";
            }

            String[] parArray = parameterString.toString().split(separator.toString());
            String parName = parameterName.toString() + "=";
            String[] var9 = parArray;
            int var8 = parArray.length;

            for(int var7 = 0; var7 < var8; ++var7) {
                String par = var9[var7];
                if (par.startsWith(parName) && par.length() > parName.length()) {
                    if (parName.equals("re=")) {
                        String partemp = par.substring(parName.length()).replace("%2A", "*");
                        String[] pars = partemp.split("\\*");
                        String var = pars.length == 2 ? (Integer.parseInt(pars[0]) > Integer.parseInt(pars[1]) ? partemp : pars[1] + "*" + pars[0]) : partemp;
                        return var;
                    }

                    return URLDecoder.decode(par.substring(parName.length())).toString();
                }
            }
        } catch (Exception var13) {
            return "#";
        }

        return "#";
    }
    public String urlDecode(String str){
        String rs = "#";
        rs = URLDecoder.decode(str);
        return rs;
    }

    public String addIdPrefix(String docid) {
        Pattern p=Pattern.compile("^((cmpp_)|(imcp_)|(video_)|(sub_))");
        Matcher m=p.matcher(docid);
        if (m.find()){
            return docid ;
        }else if ((docid.length()==7 || docid.length()==8) && docid != null && !"".equals(docid.trim()) && docid.matches("^[0-9]*$") ){
            return "sub_" + docid ;
        }else if (docid.length()==15){
            return "cmpp_" + docid ;
        }else if (docid.length()==36){
            return "video_" + docid ;
        }else{
            return docid ;
        }
    }

    public String reduceIdPrefix(String docid) {
        Pattern p=Pattern.compile("^((cmpp_)|(imcp_)|(video_)|(sub_))");
        Matcher m=p.matcher(docid);
        if (m.find()){
            return docid.split(m.group(1))[1] ;
        }else {
            return docid ;
        }
    }

    public String getVideoShareId (String id) {
        String url = "http://vcis.ifeng.com/api/videoGuid?protocol=1.0.0&shortId=" + id ;
        StringBuilder json = new StringBuilder();
        try {
            URL urlObject = new URL(url);
            URLConnection uc = urlObject.openConnection();
            BufferedReader in = new BufferedReader(new InputStreamReader(uc.getInputStream(),"UTF-8"));
            String inputLine = null;
            while ( (inputLine = in.readLine()) != null) {
                json.append(inputLine);
            }
            in.close();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            JSONObject jsonObject = new JSONObject(json.toString());
            return "video_" + jsonObject.getString("guid");
        }catch (Exception e){
            e.printStackTrace();
            return "#" ;
        }
    }



    public String getJsonValue (String jsonStr,String key){
        try {
            JSONObject jsonObject = new JSONObject(jsonStr);
            return jsonObject.getString(key).replaceAll("\n|\t|\r","");
        }catch (Exception e){
            //e.printStackTrace();
            return "#" ;
        }
    }

    static final HashMap<String, String> channel = new HashMap<String, String>();
    static {
        channel.put("apps.ifeng.com", "5001");
        channel.put("sony", "3005");
        channel.put("shengji", "1111");
        channel.put("jinliS101U", "6104");
        channel.put("meizu", "6103");
        channel.put("baiduapp", "2040");
        channel.put("tenxun", "2011");
        channel.put("sikai", "6005");
        channel.put("IfengApps", "5001");
        channel.put("IfengNew-yul", "1111");
        channel.put("cctv6", "1111");
        channel.put("IfengNew-test", "5003");
        channel.put("oppo0802", "6102");
        channel.put("UCcenter", "1007");
        channel.put("tianyi", "2029");
        channel.put("haier", "3009");
        channel.put("opera", "1031");
        channel.put("eben", "6002");
        channel.put("youpo", "1111");
        channel.put("samsungS3", "6007");
        channel.put("IfengNew-web", "5001");
        channel.put("nduo", "2023");
        channel.put("Gfan", "2008");
        channel.put("cmmm", "2026");
        channel.put("023mi", "6001");
        channel.put("moji", "1013");
        channel.put("xiaxinN90", "3001");
        channel.put("paojiao", "1015");
        channel.put("IpadNews", "4001");
        channel.put("Yicha2", "1017");
        channel.put("Yicha3", "1018");
        channel.put("Yicha1", "1016");
        channel.put("Yicha6", "1019");
        channel.put("Yicha4", "1019");
        channel.put("Yicha5", "1019");
        channel.put("AmoiN807", "3001");
        channel.put("AmoiN808", "3001");
        channel.put("asus", "6004");
        channel.put("goapk.com", "2012");
        channel.put("360.cn", "2006");
        channel.put("google_market", "2009");
        channel.put("Nduo", "2023");
        channel.put("m.ifeng.com", "1111");
        channel.put("ucweb", "1007");
        channel.put("sanxingAPP", "6007");
        channel.put("#", "1111");
        channel.put("xiaxinN810", "3001");
        channel.put("360v.cn", "1034");
        channel.put("hiapk", "2003");
        channel.put("bbg", "6101");
        channel.put("bingb", "1011");
        channel.put("TCLstore", "6106");
        channel.put("IphoneNews", "4002");
        channel.put("IfengNew", "5002");
        channel.put("windowsphone", "6013");
        channel.put("ztestore", "6108");
        channel.put("gfan", "2008");
        channel.put("SZ-lephone", "3008");
        channel.put("samsung_note", "6007");
        channel.put("android_market", "2009");
        channel.put("intel", "3006");
        channel.put("dcn", "1006");
        channel.put("91book", "1012");
        channel.put("youm", "3007");
        channel.put("HisenseM3101", "6003");
        channel.put("CoolpadYL", "6105");
        channel.put("jinl", "6104");
        channel.put("duowei", "6107");
        channel.put("yingyonghui", "2021");
        channel.put("WO-market", "2037");
        channel.put("mumayi", "2004");
    }

    public Text getPub(Text pub) {

        String site;
        try {
            site = pub.toString();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            return pub;
        }
        if (site.equals("#")) {
            return new Text("#");
        } else if (channel.containsKey(site)) {
            return new Text(channel.get(site));
        } else {
            return pub;
        }
    }
    //去除书名号
    public String smh (String source){
        if(source == null)return source;
        if(source.contains("《")){
            source = source.split("《")[1];
            if(source.contains("》")){
                source = source.split("》")[0];
            }
        }
        return source;
    }


    public static void main(String [] args){
        UDF udf = new UDF();
        udf.ucmsPrefix1("sub_6569151124568281088",true);
    }
}

