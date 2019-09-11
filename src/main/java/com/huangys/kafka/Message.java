package com.huangys.kafka;

public class Message {
    public String topic;
    public String message;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Message(String topic, String message) {
        this.topic = topic;
        this.message = message;
    }
}
