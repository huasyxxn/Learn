package com.huangys.algorithm.sort;

import com.huangys.algorithm.constant.Constants;

import javax.swing.*;
import java.awt.*;
import java.awt.geom.Rectangle2D;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;

public abstract class Sort<T> extends JPanel {
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


    void setFrame()
    {
        JFrame frame = new JFrame();
        frame.setTitle("sort");    //设置显示窗口标题
        frame.setSize(300,300);    //设置窗口显示尺寸
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);    //置窗口是否可以关闭
        Container c=frame.getContentPane();    //获取当前窗口的内容窗格
        setBackground(Color.WHITE);
        c.add(this);
        setVisible(true);
        frame.setVisible(true);    //设置窗口是否可见
    }

    @Override
    protected void paintComponent(Graphics g) {
        // TODO Auto-generated method stub
        super.paintComponent(g);
        draw(g, Constants.unSortedIntArray);
    }


    Random ran = new Random();
    void draw(Graphics g,int[] array){
        Graphics2D g2 = (Graphics2D) g;
        int width = this.getWidth();
        int height = this.getHeight();
        int h = (height-10)/array.length;
        int max = Integer.MIN_VALUE;
        for(int a : array){
            if(a>max) {
                max = a;
            }
        }
        int w = (width - 10)/max;
        int i = 0;
        for(int a : array) {
            Rectangle2D r2 = new Rectangle2D.Double(0, h*i, w*a, h);
            i++;
            g2.fill(r2);
            g2.setColor(new Color(Math.abs(225-a),Math.abs(225-a),Math.abs(225-a)));
        }
    }

//    void randomSort(){
//        List list = new ArrayList();
//        for(int i = 0;i < x.length;i++){
//            System.out.print(x[i]+", ");
//            list.add(x[i]);
//        }
//        Collections.shuffle(Constants.unSortedIntArray);
//        Constants.unSortedIntArray
//    }

    void reset(long second){
        repaint();
        try {
            Thread.sleep(second);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    void stepOver(){
        for(int i = 0 ; i < Constants.unSortedIntArray.length ; i++){
            Constants.unSortedIntArray[i] = 10;
            repaint();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
