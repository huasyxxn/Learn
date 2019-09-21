package com.huangys.algorithm.sort;

public abstract class Sort<T> {
    abstract void sort(T unSortedSet);

    /**
     * 展示方法
     */
    void display(int[] array){
        for(int i = 0;i < array.length;i++){
            System.out.print(array[i]+(i == array.length-1 ? "" : ","));
        }
        System.out.println();
    }

    void display(int[] array,int start,int end){
        for(int i = 0;i < array.length;i++){
            if(i == start){
                System.out.print("|->"+array[i] + (i == array.length - 1 ? "" : ","));
            }else if(i == end){
                System.out.print(array[i] + (i == array.length - 1 ? "" : ",")+"<-|");
            }else {
                System.out.print(array[i] + (i == array.length - 1 ? "" : ","));
            }
        }
        System.out.println();
    }

    /**
     * 算法的一步完成时，调用此方法
     */
    void stepOver(){

    }
}
