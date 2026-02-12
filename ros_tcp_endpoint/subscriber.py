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

        qos_profile = self.get_matched_qos(topic, queue_size)

        # Start Subscriber listener function
        self.subscription = self.create_subscription(
            self.msg, self.topic, self.send, qos_profile  # queue_size
        )
        self.subscription

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
        self.destroy_subscription(self.subscription)
        self.destroy_node()

    def get_matched_qos(self, topic, queue_size):
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
        except Exception as e:
            self.get_logger().warn(
                f"Failed to match QoS for topic {topic}: {e}, using default QoS"
            )

        return qos_profile
