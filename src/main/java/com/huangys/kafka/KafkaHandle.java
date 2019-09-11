package com.huangys.kafka;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *  kafka数据处理抽象类
 */
public class KafkaHandle extends Thread{
    /**
     * Kafka配置参数
     */
    protected Properties kafkaProperties = null;
    /**
     * kafka消费者
     */
    protected KafkaConsumer<String, String> kafkaConsumer = null;
    /**
     * handle 数据队列 默认容量为10
     */
    BlockingQueue<String> dataQueue = new ArrayBlockingQueue<>(10);
    /**
     * 提交offset信号值
     */
    boolean commitAsync = false;
    boolean commitSync = false;

    boolean stop = false;

    /**
     * 构造方法1
     * @param kafkaProperties Kafka配置参数,若传入值为Null,抛出空指针异常
     */
    public KafkaHandle(Properties kafkaProperties){
        if(kafkaProperties == null){
            throw new NullPointerException("参数不可为Null");
        }
        this.kafkaProperties = kafkaProperties;
        consumerInit();
    }

    /**
     * 构造方法2
     * @param bootstrapServers kafka地址
     * @param groupId   kafka消费者组
     * @param autoCommit 自动提交
     * @param offsetReset 偏移量
     * @param topics 订阅主题
     */
    public KafkaHandle(String bootstrapServers,String groupId,String autoCommit,String offsetReset,String ... topics){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);//xxx是服务器集群的ip
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", autoCommit);
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", offsetReset);
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("kafkaHandle.topics", StringUtils.join(topics,","));
        this.kafkaProperties = properties;
        consumerInit();
    }

    /**
     * 初始化方法 线程开始时调用
     */
    public void init(){

    }

    /**
     * 不使用默认dataQueue
     * @param dataQueue 传入数据队列
     */
    public void setDataQueue(BlockingQueue<String> dataQueue){
        this.dataQueue = dataQueue;
    }

    /**
     *
     * @param dataQueueSize 自定义dataQueue容量
     */
    public void setDataQueue(int dataQueueSize){
        this.dataQueue = new ArrayBlockingQueue<>(dataQueueSize);;
    }

    /**
     *
     * @return dataQueue数据
     */
    public String getData(){
        try {
            return dataQueue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     *
     * @param message 将消息发布到dataQueue
     */
    protected void publish(String message){
        try {
            this.dataQueue.put(message);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param handles 并发运行handles
     */
    public static KafkaHandle[] runKafkaHandle(KafkaHandle...handles){
        int handleNum = handles.length;
        BlockingQueue blockingQueue=new ArrayBlockingQueue<>(handleNum);
        ThreadPoolExecutor threadPoolExecutor=new ThreadPoolExecutor(handleNum, handleNum*2, 1, TimeUnit.MINUTES, blockingQueue);
        for(KafkaHandle handle:handles){
            threadPoolExecutor.submit(handle);
        }
        threadPoolExecutor.shutdown();
        return handles;
    }

    /**
     * 将数组中handle的提交信号值置为true
     */
    public static void commitAsync(KafkaHandle[] handles){
        for(KafkaHandle handle:handles){
            handle.commitAsync = true;
        }
    }

    public static void commitSync(KafkaHandle[] handles){
        for(KafkaHandle handle:handles){
            handle.commitSync = true;
        }
    }


    public void commitAsync(){
        kafkaConsumer.commitAsync();
    }
    public void commitSync(){
        kafkaConsumer.commitSync();
    }

    public void commitAsyncByFlag(){
        if(commitAsync == true){
            kafkaConsumer.commitAsync();
        }
    }
    public void commitSyncByFlag(){
        if(commitSync == true) {
            kafkaConsumer.commitSync();
        }
    }

    /**
     * 消费者初始化
     */
    public KafkaConsumer<String, String> consumerInit(){
        if(kafkaConsumer == null) {
            this.kafkaConsumer = new KafkaConsumer<String, String>(this.kafkaProperties);
            //订阅topic
            kafkaConsumer.subscribe(Arrays.asList(this.kafkaProperties.getProperty("kafkaHandle.topics").split(",")));
        }
        return this.kafkaConsumer;
    }

    /**
     * 实现业务代码
     * 处理后非Null数据将被放入dataQueue 若不想发布数据 可以返回Null
     */
    String handle(ConsumerRecord<String,String> record){
        return record.value();
    }

    public void startHandle(){
        this.start();
    }
    public void stopThisThread(){
        this.stop = true;
    }

    @Override
    public void run(){
        super.setName("kafkaHandle-"+super.getName());
        consumerInit();
        init();
        while(true){
            ConsumerRecords<String, String> records =  kafkaConsumer.poll(10);
            for (ConsumerRecord<String,String> record:records){
                String handleResult = handle(record);
                if(handleResult != null){
                    publish(handle(record));
                }
            }
            if(this.stop){
                break;
            }
        }
    }
}

