import threading, logging, time
import multiprocessing

from kafka import KafkaConsumer

class Consumer(multiprocessing.Process):
   def __init__(self):
       multiprocessing.Process.__init__(self)
       self.stop_event = multiprocessing.Event()

   def stop(self):
       self.stop_event.set()

   def run(self):
       consumer = KafkaConsumer('processed_stream',
                                bootstrap_servers=['150.82.218.237:9092'],
                                auto_offset_reset='latest',
                                api_version=(0, 10),
                                consumer_timeout_ms=1000)
       consumer.subscribe(['processed_stream'])

       while not self.stop_event.is_set():
           for message in consumer:
               print(message)
               if self.stop_event.is_set():
                   break

       consumer.close()


def main():
   tasks = [
       Consumer()
   ]

   for t in tasks:
       t.start()

   time.sleep(100)

   for task in tasks:
       task.stop()

   for task in tasks:
       task.join()


if __name__ == "__main__":
   logging.basicConfig(
       level=logging.INFO
   )
   main()