package com.huangys.algorithm.sort;

import com.huangys.algorithm.constant.Constants;

/**
 * 选择排序：
 * 检查起点设为零
 * 从左到右找最小元素，记录位置
 * 最小值和检查起点替换
 * 检查起点加一
 * 重复
 *
 * O(n^2)
 */
public class SelectSort extends Sort<int []> {
    @Override
    void sort(int[] unSortedSet) {
        display(unSortedSet);

        for(int i = 0;i<unSortedSet.length;i++){
            int minValue = unSortedSet[i];
            int minIndex = i;
            for(int j = i+1;j<unSortedSet.length;j++){
                if(minValue>unSortedSet[j]){
                    minIndex = j;
                    minValue = unSortedSet[j];
                }
            }
            int tmp = unSortedSet[i];
            unSortedSet[i] = unSortedSet[minIndex];
            unSortedSet[minIndex] = tmp;

            display(unSortedSet);
        }

        display(unSortedSet);
    }

    public static void main(String[] args) {
        SelectSort selectSort = new SelectSort();
        selectSort.sort(Constants.unSortedIntArray);
    }
}
