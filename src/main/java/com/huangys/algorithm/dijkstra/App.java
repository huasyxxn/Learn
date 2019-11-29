package com.huangys.algorithm.dijkstra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

public class App {
    public static Map<String, Integer> newMap(){
        Map<String, Integer> map = new Map<>();
        map.initMap("B","C","D","E","F","G","H","I","A","J","K","L","M","N","P","O","X","R","T","S","Z","Y");
        map.setEdge("A","B",10);
        map.setEdge("A","E",7);
        map.setEdge("A","D",8);
        map.setEdge("B","E",3);
        map.setEdge("E","C",5);
        map.setEdge("E","F",8);
        map.setEdge("D","F",12);
        map.setEdge("D","N",10);
        map.setEdge("C","F",3);
        map.setEdge("G","F",30);
        map.setEdge("G","N",25);
        map.setEdge("G","H",10);
        map.setEdge("G","I",12);
        map.setEdge("G","J",20);
        map.setEdge("N","M",18);
        map.setEdge("N","H",20);
        map.setEdge("M","H",15);
        map.setEdge("M","L",30);
        map.setEdge("M","K",30);
        map.setEdge("H","L",15);
        map.setEdge("H","K",10);
        map.setEdge("H","I",2);
        map.setEdge("I","J",8);
        map.setEdge("I","K",3);
        map.setEdge("I","L",20);
        map.setEdge("J","K",10);
        map.setEdge("K","L",8);
        map.setEdge("E","D",4);
        map.setEdge("P","A",20);
        map.setEdge("P","D",30);
        map.setEdge("P","N",35);
        map.setEdge("O","M",30);
        map.setEdge("O","N",40);
        map.setEdge("R","B",30);
        map.setEdge("R","C",35);
        map.setEdge("R","T",8);
        map.setEdge("R","X",3);
        map.setEdge("R","Z",2);
        map.setEdge("R","S",8);
        map.setEdge("T","S",5);
        map.setEdge("T","C",30);
        map.setEdge("T","B",35);
        map.setEdge("T","J",50);
        map.setEdge("X","Z",2);
        map.setEdge("X","Y",5);
        map.setEdge("Z","Y",2);
        map.setEdge("Z","S",3);
        map.setEdge("S","Y",4);
        return map;
    }
    public static void main(String[] args) {
        Map<String,Integer> map = newMap();
//        map.breadthFirstLoop("A",new LinkedList<String>(),new HashSet<String>());
        String src = "B";
        HashMap<String, Double> rs = new HashMap<>();
        rs.put(src, 0.0);
        HashMap<String, ArrayList<String>> roadListMap = new HashMap<String, ArrayList<String>>();
        map.dijkstra(src,rs,new HashMap<String, Double>(),roadListMap,new HashMap<String,String>());
//        map.dijkstra("B",rs,new HashMap<String, Double>(),roadListMap,new HashMap<String, String>(),"C");
        for(java.util.Map.Entry<String, ArrayList<String>> entry : roadListMap.entrySet()){
            System.out.print(entry.getKey()+": "+src+"->");
            for (String key : entry.getValue()) {
                System.out.print(key+"->");
            }
            System.out.println(rs.get(entry.getKey()));
        }
    }
}
