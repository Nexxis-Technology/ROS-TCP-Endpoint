#  Copyright 2020 Unity Technologies
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


import re

from rclpy.qos import QoSProfile

from .communication import RosReceiver

QOS_CHECK_TIMERR_PERIOD = 2.0


class RosSubscriber(RosReceiver):
    """
    Class to send messages outside of ROS network
    """

    def __init__(self, topic, message_class, tcp_server, queue_size=2):
        """

        Args:
            topic:         Topic name to publish messages to
            message_class: The message class in catkin workspace
            queue_size:    Max number of entries to maintain in an outgoing queue
        """
        strippedTopic = re.sub("[^A-Za-z0-9_]+", "", topic)
        self.node_name = f"{strippedTopic}_RosSubscriber"
        RosReceiver.__init__(self, self.node_name)
        self.topic = topic
        self.msg = message_class
        self.tcp_server = tcp_server
        self.queue_size = queue_size
        self.check_qos_timer = None

        qos_profile = self.get_matched_qos(self.topic, self.queue_size)

        self.current_qos = (qos_profile.reliability, qos_profile.durability)

        # Start Subscriber listener function
        self.subscription = self.create_subscription(
            self.msg, self.topic, self.send, qos_profile  # queue_size
        )

    def send(self, data):
        """
        Connect to TCP endpoint on client and pass along message
        Args:
            data: message data to send outside of ROS network

        Returns:
            self.msg: The deserialize message

        """
        self.tcp_server.send_unity_message(self.topic, data)
        return self.msg

    def unregister(self):
        """

        Returns:

        """
        if self.check_qos_timer:
            self.destroy_timer(self.check_qos_timer)
        self.destroy_subscription(self.subscription)
        self.destroy_node()

    def get_matched_qos(self, topic: str, queue_size: int) -> QoSProfile:
        """Match QoS to existing publishers on the topic"""
        qos_profile = QoSProfile(depth=queue_size)
        try:
            pub_info = self.get_publishers_info_by_topic(topic)
            if pub_info:
                source_qos = pub_info[0].qos_profile
                qos_profile.reliability = source_qos.reliability
                qos_profile.durability = source_qos.durability
                self.get_logger().info(
                    f"Matched QoS for {topic}: reliability={source_qos.reliability}, durability={source_qos.durability}"
                )
                return qos_profile
            else:
                self.get_logger().warn(
                    f"No publisher found for topic {topic}, using default QoS"
                )
                if not self.check_qos_timer:
                    self.check_qos_timer = self.create_timer(
                        QOS_CHECK_TIMERR_PERIOD, self.check_qos_match
                    )
        except Exception as e:
            self.get_logger().warn(
                f"Failed to match QoS for topic {topic}: {e}, using default QoS"
            )

        return qos_profile

    def check_qos_match(self):
        try:
            pub_info = self.get_publishers_info_by_topic(self.topic)
            if pub_info:
                source_qos = pub_info[0].qos_profile
                new_qos = (source_qos.reliability, source_qos.durability)
                if self.current_qos != new_qos:
                    self.get_logger().warn(
                        f"Publisher QoS changed for {self.topic}, recreating subscription"
                    )
                    self.destroy_subscription(self.subscription)
                    qos_profile = self.get_matched_qos(self.topic, self.queue_size)
                    self.subscription = self.create_subscription(
                        self.msg, self.topic, self.send, qos_profile
                    )
                    self.current_qos = new_qos
                self.destroy_timer(self.check_qos_timer)
                self.check_qos_timer = None
        except Exception as e:
            self.get_logger().debug(f"QoS check failed for {self.topic}: {e}")
