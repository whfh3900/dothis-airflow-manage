
import time
import json
import logging
from datetime import datetime
from typing import Optional, Dict, Union, List
from confluent_kafka import Consumer, Producer, TopicPartition, KafkaException


def kafka_offset_checker(topic: str, group_id: str, num_partitions: int):

    broker_list = ["dothis2.iptime.org:19092", "dothis2.iptime.org:29092", "dothis2.iptime.org:39092"]
    consumer = Consumer({
        'bootstrap.servers': ','.join(broker_list),
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    })

    topic_partitions = [TopicPartition(topic, partition=i) for i in range(num_partitions)]
    offset_end_count = 0
    is_not_done = True

    while is_not_done:
        current_offsets = consumer.committed(topic_partitions)
        offset_end_count = 0  # Reset the count for each check

        for i in range(num_partitions):
            topic_partition = TopicPartition(topic, partition=i)
            _, high_watermark = consumer.get_watermark_offsets(topic_partition)
            if current_offsets[i].offset != -1001 and high_watermark == current_offsets[i].offset:
                offset_end_count += 1

        if offset_end_count >= num_partitions:
            is_not_done = False
        else:
            time.sleep(2)  # Wait for a while before the next check

    consumer.close()
    return True

def kafka_topic_end_producer(topic: str, num_partitions: int, content: str = "this topic is end"):
    """ 카프카 토픽이 종료되면 해당 토픽에 Topic이 종료되었다는 메시지를 모든 파티션에 발행한다."""
    broker_list = ["dothis2.iptime.org:19092", "dothis2.iptime.org:29092", "dothis2.iptime.org:39092"]
    today = datetime.today().strftime("%Y-%m-%d")
    conf = {
        'bootstrap.servers': ",".join(broker_list),
        'acks': -1,
    }
    p = Producer(conf)
    for partition_no in range(num_partitions):
        message = {
            'date': today,
            'msg': 'done',
            'partition_no': partition_no,
            'content': content
        }
        p.produce(
            topic=topic,
            value=json.dumps(message).encode("utf-8"),
            partition=partition_no
        )
    p.flush()

class SimpleKafkaProducer:

    def __init__(self, topic: str, acks: int = -1, num_partitions: int = 3):
        self.topic = topic
        self.num_partitions = num_partitions
        self.producer = Producer({
            'bootstrap.servers': ",".join(["dothis2.iptime.org:19092", "dothis2.iptime.org:29092", "dothis2.iptime.org:39092"]),
            'acks': acks,
        })
    
    def __delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        
        if err is not None:
            logging.warning(f"Message delivery failed Error: {err}")
            logging.warning(f"msg info: topic: {msg.topic()} value: {msg.value()}")
        else:
            pass
        
    def stream(self, *value):
        value = json.dumps(list(value)[0])
        p = self.producer
        p.produce(
            topic=self.topic,
            value=value.encode("utf-8"),
            callback=self.__delivery_report,
        )
        p.flush()

    def produce(self, values: list):
        p = self.producer
        current_partition = 0
        flush_amount = 0
        for v in values:
            if isinstance(v, dict):
                v = json.dumps(v, ensure_ascii=False)
                
            p.produce(
                topic=self.topic,
                value=v.encode("utf-8"),
                callback=self.__delivery_report,
                partition=current_partition
            )
            current_partition = (current_partition + 1) % self.num_partitions
            flush_amount += 1
            if flush_amount >= 90000:
                p.flush()
                flush_amount = 0
        p.flush()

class SimpleKafkaConsumer:
    def __init__(self, topic: str, num_partitions: int = 3, 
                 group_id: str = "default-consumer", client_id: str = "default-client",
                 consume_size: int = 20000):
        self.consume_size = consume_size
        self.group_id = group_id
        self.topic = topic
        self.num_partitions = num_partitions
        self.broker_list = ["dothis2.iptime.org:19092", "dothis2.iptime.org:29092", "dothis2.iptime.org:39092"]
        self.consumer = Consumer({
            'bootstrap.servers': ",".join(self.broker_list),
            'group.id': group_id,
            'client.id': client_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False  # 수동으로 커밋하기 위해 자동 커밋 비활성화
        })
        self.consumer.subscribe([self.topic])
    def consume(self):
        result = []
        try:
            while True:
                if isinstance(self.consume_size, int) and len(result) >= self.consume_size:
                    break  # 목표 consume_size 도달시 consume 종료

                msg = self.consumer.poll(5)
                
                if msg is None:
                    is_all_consume = kafka_offset_checker(
                        topic=self.topic,
                        group_id=self.group_id,
                        num_partitions=self.num_partitions,
                    )
                    if is_all_consume:
                        break
                    else:
                        continue
                    
                if msg.error():
                    print(f"Consume Error: {msg.error()}")

                try:
                    value = json.loads(msg.value().decode('utf-8'))
                    # 실제로 메시지 처리하는 로직을 구현하고 결과를 result 리스트에 추가
                    result.append(value)
                except Exception as e:
                    print(f"Message processing failed: {e}")
                    continue  # 다음 메시지 처리로 넘어감

                # 메시지 처리가 성공했을 때 오프셋을 수동으로 커밋
                self.consumer.commit(msg)

        except KeyboardInterrupt:
            pass

        return result if result else None
    def close(self):
        self.consumer.close()

class KafkaEndCheck:

    def __init__(self, partition_cnt: int):
        self.partition_status = {no: False for no in range(partition_cnt)}
    
    def done(self, partition_no: int) -> None:
        # 파티션 번호가 유효한지 확인 후 상태 업데이트
        if partition_no in self.partition_status:
            self.partition_status[partition_no] = True
    
    def check(self) -> bool:
        # 모든 파티션이 True(처리 완료)인지 확인
        return all(self.partition_status.values())