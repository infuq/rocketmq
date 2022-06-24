package org.apache.rocketmq.example.quickstart;

import java.util.List;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

public class Consumer {

    public static void main(String[] args) throws MQClientException {


        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("m_consumer_group");

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setNamesrvAddr("192.168.0.100:9876");
        consumer.subscribe("TOPIC_TEST_2", "*");

if (1==1) {
    consumer.registerMessageListener(new MessageListenerConcurrently() {

        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            System.out.println("该批次消息数量=" + msgs.size());
            for (MessageExt v : msgs) {
//                    System.out.println("接收消息: " + JSON.toJSONString(v));
                System.out.println("接收消息: " + JSON.toJSONString(v.getStoreHost()) + ", msgId=" + v.getMsgId() + ", queueId=" + v.getQueueId());
//                    System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
            }

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
    });
}
else {
    consumer.registerMessageListener(new MessageListenerOrderly() {
        @Override
        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
            System.out.println("该批次消息数量=" + msgs.size());
            for (MessageExt v : msgs) {
                System.out.println("接收消息: " + JSON.toJSONString(v.getStoreHost()) + ", msgId=" + v.getMsgId() + ", queueId=" + v.getQueueId());
            }
            return ConsumeOrderlyStatus.SUCCESS;
        }
    });
}

        consumer.start();

        System.out.printf("Consumer Started.%n");
    }

}
