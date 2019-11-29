package com.huangys.algorithm.dijkstra;

import java.util.*;

public class Map<T,E extends Number> {
    public LinkedHashMap<T, Node> mapList = new LinkedHashMap<>();

    public void dijkstra(T src,T target,HashMap<T, Double> rs,HashMap<T, ArrayList<T>> roadListMap){
        dijkstra(src,rs,new HashMap<T, Double>(),roadListMap,new HashMap<T,T>(),target);
    }

    public void dijkstra(T current,HashMap<T,Double> rs,HashMap<T,Double> tmp,HashMap<T,ArrayList<T>> roadListMap,HashMap<T,T> preNode,T ... target){
        Double currentRange = rs.get(current);
        Node node = mapList.get(current);
        Iterator<java.util.Map.Entry<T, E>> it = node.getIt();
        java.util.Map.Entry<T, E> next = null;
        while(null !=(next = node.scan(it))){
            if(rs.containsKey(next.getKey())){
                continue;
            }
            if(tmp.getOrDefault(next.getKey(),Double.MAX_VALUE) > (currentRange.doubleValue() + next.getValue().doubleValue())){
                tmp.put(next.getKey(),currentRange.doubleValue() + next.getValue().doubleValue());
                preNode.put(next.getKey(), current);
            }
        }

        while(!tmp.isEmpty()) {
            T minKey = null;
            Double minValue = Double.MAX_VALUE;
            for (java.util.Map.Entry<T, Double> entry : tmp.entrySet()) {
                if (entry.getValue() < minValue) {
                    minKey = entry.getKey();
                    minValue = entry.getValue();
                }
            }
            tmp.remove(minKey);
            rs.put(minKey, minValue);
            ArrayList<T> roadlist = (ArrayList<T>)(roadListMap.getOrDefault(preNode.get(minKey),new ArrayList<T>()).clone());
            roadlist.add(minKey);
            roadListMap.put(minKey,roadlist);
            if(target.length > 0){
                if(minKey.equals(target[0])){
                    target[0] = null;
                    return;
                }else if(target[0] == null){
                    return;
                }
            }
            dijkstra(minKey,rs,tmp,roadListMap,preNode,target);
        }
    }
    private void displayMap(HashMap<T,Double> rs){
        for(java.util.Map.Entry<T, Double> entry : rs.entrySet()){
            System.out.print(entry.getKey()+":"+entry.getValue()+",");
        }
        System.out.println();
    }

    public void  breadthFirstLoop(T firstNode,Queue<T> queue,Set<T> isLoop){
        isLoop.add(firstNode);
        java.util.Map.Entry<T, E> node = null;
        Iterator<java.util.Map.Entry<T, E>> it = mapList.get(firstNode).getIt();
        while(null !=(node = mapList.get(firstNode).scan(it))){
            if(!isLoop.contains(node.getKey())) {
                queue.add(node.getKey());
                isLoop.add(node.getKey());
            }
            System.out.println(firstNode+"->"+node.getKey()+" : "+node.getValue());
        }
        if(queue.isEmpty()){
            return;
        }else{
            breadthFirstLoop(queue.poll(),queue,isLoop);
        }
    }

    public void initMap(T...keys){
        for(T key : keys){
            mapList.put(key,new Node(key));
        }
    }
    public void setEdge(T node1,T node2,E value){
        setEdge(node1,node2,value,value);
    }
    public void setEdge(T node1,T node2,E value1,E value2){
        if(!mapList.containsKey(node1) || !mapList.containsKey(node2)){
            throw new RuntimeException("unknown node : "+node1+"/"+node2);
        }
        if(value1 != null) {
            mapList.get(node1).add(node2, value1);
        }
        if(value2 != null) {
            mapList.get(node2).add(node1, value2);
        }
    }
    class Node {
        private T key;
        private HashMap<T, E> map = new HashMap<T, E>();
        public Node(T key){
            this.key = key;
        }
        public T getKey(){
            return key;
        }
        public E getNode(T key){
            return map.get(key);
        }
        public void add(T next, E value) {
            map.put(next, value);
        }

        public void delete(T next) {
            map.remove(next);
        }

        public Iterator<java.util.Map.Entry<T, E>> getIt() {
            return map.entrySet().iterator();
        }

        public java.util.Map.Entry<T, E> scan(Iterator<java.util.Map.Entry<T, E>> it) {
            if (it == null) {
                it = map.entrySet().iterator();
            }
            if (!it.hasNext()) {
                return null;
            }
            return it.next();
        }
    }
}
