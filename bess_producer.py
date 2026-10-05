"""
BESS (Battery Energy Storage System) Kafka Producer
Simulates 30 BESS units producing voltage, current, temperature, and SOC data
"""

import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BESSProducer:
    """
    Simulates a BESS (Battery Energy Storage System) that produces
    voltage, current, temperature, and SOC (State of Charge) data to Kafka
    """
    
    def __init__(self, bootstrap_servers='kafka:29092', num_units=30):
        """
        Initialize the BESS Producer
        
        Args:
            bootstrap_servers (str): Kafka broker address
            num_units (int): Number of BESS units to simulate
        """
        self.bootstrap_servers = bootstrap_servers
        self.num_units = num_units
        self.topic = 'bess-data'
        
        # Initialize Kafka producer
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',  # Wait for all replicas to acknowledge
                retries=3
            )
            logger.info(f"Successfully connected to Kafka at {bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def generate_bess_data(self, unit_id):
        """
        Generate realistic BESS sensor data for a single unit
        
        Args:
            unit_id (int): The ID of the BESS unit
            
        Returns:
            dict: Dictionary containing BESS sensor readings
        """
        # Voltage: 200-600V (realistic range for battery systems)
        voltage = round(random.uniform(200, 600), 2)
        
        # Current: -100 to +100 A (negative = discharging, positive = charging)
        current = round(random.uniform(-100, 100), 2)
        
        # Temperature: 15-45°C (typical operating range)
        temperature = round(random.uniform(15, 45), 2)
        
        # State of Charge (SOC): 0-100%
        soc = round(random.uniform(0, 100), 2)
        
        # Timestamp in ISO format
        timestamp = datetime.utcnow().isoformat() + 'Z'
        
        # Construct the message
        message = {
            'id': f'BESS-{unit_id:03d}',  # Format: BESS-001, BESS-002, etc.
            'timestamp': timestamp,
            'voltage_v': voltage,
            'current_a': current,
            'temperature_c': temperature,
            'soc_percent': soc
        }
        
        return message
    
    def send_message(self, message, unit_id):
        """
        Send a single message to Kafka
        
        Args:
            message (dict): The message to send
            unit_id (int): The BESS unit ID (for logging purposes)
        """
        try:
            # Send message to the topic
            self.producer.send(self.topic, value=message)
            logger.info(f"Sent message for BESS-{unit_id:03d}: V={message['voltage_v']}V, "
                       f"I={message['current_a']}A, T={message['temperature_c']}°C, "
                       f"SOC={message['soc_percent']}%")
        except Exception as e:
            logger.error(f"Failed to send message for BESS-{unit_id:03d}: {e}")
    
    def create_topic_if_not_exists(self):
        """
        Create the Kafka topic if it doesn't already exist
        This is optional - Kafka can auto-create topics
        """
        try:
            from kafka.admin import KafkaAdminClient, NewTopic
            
            admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
            
            # Define the topic
            topic = NewTopic(
                name=self.topic,
                num_partitions=3,
                replication_factor=1
            )
            
            # Try to create the topic
            admin_client.create_topics(new_topics=[topic], validate_only=False)
            logger.info(f"Topic '{self.topic}' created successfully")
            admin_client.close()
        except Exception as e:
            # Topic might already exist, which is fine
            logger.info(f"Topic creation skipped (might already exist): {e}")
    
    def start_producing(self, interval_seconds=5):
        """
        Start the producer loop - continuously send data from all BESS units
        
        Args:
            interval_seconds (int): Time in seconds between message batches
        """
        logger.info(f"Starting BESS Producer with {self.num_units} units")
        logger.info(f"Messages will be sent every {interval_seconds} seconds")
        logger.info(f"Sending to Kafka topic: '{self.topic}'")
        
        try:
            while True:
                # Generate and send data for each BESS unit
                for unit_id in range(1, self.num_units + 1):
                    # Generate realistic sensor data
                    message = self.generate_bess_data(unit_id)
                    
                    # Send to Kafka
                    self.send_message(message, unit_id)
                
                # Wait before sending the next batch of messages
                logger.info(f"Waiting {interval_seconds} seconds before next batch...")
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("Producer stopped by user (Ctrl+C)")
        except Exception as e:
            logger.error(f"Producer encountered an error: {e}")
        finally:
            # Flush any remaining messages and close the producer
            self.producer.flush()
            self.producer.close()
            logger.info("Producer closed")


def main():
    """Main entry point for the BESS Producer"""
    # Create producer instance
    producer = BESSProducer(bootstrap_servers='kafka:29092', num_units=30)
    
    # Create topic (optional, but good practice)
    producer.create_topic_if_not_exists()
    
    # Start producing messages every 5 seconds
    producer.start_producing(interval_seconds=5)


if __name__ == '__main__':
    main()
