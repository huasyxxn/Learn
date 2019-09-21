package com.huangys.algorithm.sort;

import com.huangys.algorithm.constant.Constants;

/**
 * 快速排序
 * 分而治之
 * 在数组中找到一个值
 * 指针1从左向右找大于基准的数据，找到则停止
 * 指针2从右向左找小于基准的数据，找到则停止
 * 若两指针不重合，交换
 * 若两指针重合，返回
 *
 * O(n*logN) O(n^2)(最坏情况下)
 */
public class FastSort extends Sort<int[]> {
    @Override
    void sort(int[] unSortedSet) {
        display(unSortedSet);
        sort(unSortedSet,0,unSortedSet.length-1);
        display(unSortedSet);
    }

    void sort(int[] unSortedSet,int start,int end){
        if(start >= end){
            return;
        }
        int flagValue = unSortedSet[end];
        int right = end - 1;
        int left = start;
        System.out.println("flag:"+flagValue+" start:"+start+" end:"+end);
        display(unSortedSet,start,end);
        while (true){
            while(unSortedSet[left] < flagValue && left<end){
                left ++;
            }
            while(unSortedSet[right] > flagValue && right>=start){
                right --;
            }
            if(right <= left){
                int tmp = unSortedSet[left];
                unSortedSet[left] = flagValue;
                unSortedSet[end] = tmp;
                break;
            }
            System.out.println(left+","+unSortedSet[left]+"\t"+right+","+unSortedSet[right]);
            int tmp = unSortedSet[right];
            unSortedSet[right] = unSortedSet[left];
            unSortedSet[left] = tmp;
            display(unSortedSet,start,end);
        }
        display(unSortedSet,start,end);
        System.out.println("-------------------");
        sort(unSortedSet,start,right);
        sort(unSortedSet,right+1,end);
    }

    int select(int[] unSortedSet,int start,int end,int index){
        int flagValue = unSortedSet[end];
        int right = end - 1;
        int left = start;
        System.out.println("flag:"+flagValue+" start:"+start+" end:"+end);
        display(unSortedSet,start,end);
        while (true){
            while(unSortedSet[left] < flagValue && left<end){
                left ++;
            }
            while(unSortedSet[right] > flagValue && right>=start){
                right --;
            }
            if(right <= left){
                int tmp = unSortedSet[left];
                unSortedSet[left] = flagValue;
                unSortedSet[end] = tmp;
                break;
            }
            System.out.println(left+","+unSortedSet[left]+"\t"+right+","+unSortedSet[right]);
            int tmp = unSortedSet[right];
            unSortedSet[right] = unSortedSet[left];
            unSortedSet[left] = tmp;
            display(unSortedSet,start,end);
        }
        display(unSortedSet,start,end);
        System.out.println("-------------------");
        if(left == index){
            return left;
        }
        System.out.println("right:"+right+" end:"+end);
        return start >= right?select(unSortedSet,right+1,end,index):select(unSortedSet,start,right,index);
    }

    public static void main(String[] args) {
        FastSort fastSort = new FastSort();
//        fastSort.sort(Constants.unSortedIntArray);
        int i = fastSort.select(Constants.unSortedIntArray,0,Constants.unSortedIntArray.length-1,2);
        System.out.println(Constants.unSortedIntArray[i]);
    }
}
