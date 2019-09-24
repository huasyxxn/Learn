package com.huangys.algorithm.sort;

import com.huangys.algorithm.constant.Constants;

/**
 * 插入排序：
 * 设置起始位置，值放入内存
 * 从左向右比较左侧数据，大于起始数据则右移
 * 起始位置加一重复
 * O(n^2)
 */
public class InsertSort extends Sort<int []>{
    @Override
    void sort(int[] unSortedSet) {
        display(unSortedSet);
        for(int i = 1;i<unSortedSet.length;i++){
            int tmpValue = unSortedSet[i];
            int index = i;
            while (index >0 && unSortedSet[index - 1]>tmpValue){
                unSortedSet[index] = unSortedSet[index - 1];
                index -- ;
                reset(500);
            }
            unSortedSet[index] = tmpValue;
            reset(500);
            display(unSortedSet);
        }
        display(unSortedSet);
    }

    public static void main(String[] args) {
        InsertSort insertSort = new InsertSort();
        insertSort.setFrame();
        insertSort.sort(Constants.unSortedIntArray);
    }
}
