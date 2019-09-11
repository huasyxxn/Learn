package com.huangys.kafka;

import org.apache.kafka.clients.producer.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class KafkaProducerHandle extends Thread {
    /**
     * 缓存队列
     */
    BlockingQueue<Message> dataQueue = new ArrayBlockingQueue<Message>(10);
    KafkaProducer<Integer,String> producer = null;
    Properties kafkaProperties = new Properties();

    public KafkaProducerHandle(Properties kafkaProperties){
        this.kafkaProperties = kafkaProperties;
        init();
    }

    public KafkaProducerHandle(String bootServer){
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootServer);
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        init();
    }

    /**
     * 初始化生产者
     */
    public void init(){
        if(producer == null){
            producer = new KafkaProducer<Integer, String>(kafkaProperties);
        }
    }

    public void setDataQueue(BlockingQueue<Message> dataQueue){
        this.dataQueue = dataQueue;
    }

    public void addData(Message message){
        try {
            dataQueue.put(message);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void send(){
        try {
            Message message = dataQueue.take();
            producer.send(new ProducerRecord<Integer, String>(message.getTopic(),message.getMessage()));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run(){
        init();
        while (true){
            send();
        }
    }

    @Override
    public void start(){
        if(!this.isAlive()){
            super.start();
        }
    }

    public static void main(String[] args) {

    }
}

