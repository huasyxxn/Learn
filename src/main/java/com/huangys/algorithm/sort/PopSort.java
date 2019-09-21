package com.huangys.algorithm.sort;

import com.huangys.algorithm.constant.Constants;

public class PopSort extends Sort<int[]>{
    /**
     * 冒泡排序
     * O(n^2) 交换+比较 = （1+2+3+...+n）*2
     * @param unSortedSet 待排序集合
     */
    @Override
    void sort(int[] unSortedSet) {
        display(unSortedSet);
        int len = unSortedSet.length;
        for(int i = 1;i<len;i++){
            for(int j = 0;j<len-i;j++){
                if(unSortedSet[j]>unSortedSet[j+1]){
                    int tmp = unSortedSet[j];
                    unSortedSet[j] = unSortedSet[j+1];
                    unSortedSet[j+1] = tmp;
                }
            }
        }
        display(unSortedSet);
    }

    public static void main(String[] args) {
        PopSort popSort = new PopSort();
        popSort.sort(Constants.unSortedIntArray);
    }
}
