"""
ROSbag processor for RoboLake CLI

Handles reading and converting ROSbag files to various formats.
"""

from pathlib import Path
from typing import List, Optional, Dict, Any
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging

# Try to import rosbags
try:
    from rosbags.highlevel import AnyReader
    from rosbags.typesys import Stores, get_typestore
    ROSBAGS_AVAILABLE = True
except ImportError:
    ROSBAGS_AVAILABLE = False
    logging.warning("rosbags library not available. Install with: pip install 'robolake-cli[rosbags]'")

class ROSbagProcessor:
    """Processes ROSbag files and converts to various formats"""
    
    def __init__(self, bag_path: str):
        self.bag_path = Path(bag_path)
        if not self.bag_path.exists():
            raise FileNotFoundError(f"ROSbag file not found: {bag_path}")
        
        self.metadata = self._extract_metadata()
    
    def _extract_metadata(self) -> Dict[str, Any]:
        """Extract metadata from ROSbag file"""
        if not ROSBAGS_AVAILABLE:
            return {
                "topics": [],
                "message_count": 0,
                "duration": 0.0,
                "start_time": None,
                "end_time": None,
                "error": "rosbags library not available"
            }
        
        try:
            return self._extract_rosbags_metadata()
        except Exception as e:
            logging.error(f"Error extracting metadata: {e}")
            return {
                "topics": [],
                "message_count": 0,
                "duration": 0.0,
                "start_time": None,
                "end_time": None,
                "error": str(e)
            }
    
    def _extract_rosbags_metadata(self) -> Dict[str, Any]:
        """Extract metadata using rosbags library"""
        typestore = get_typestore(Stores.ROS2_FOXY)
        
        with AnyReader([self.bag_path], default_typestore=typestore) as reader:
            topics = [conn.topic for conn in reader.connections]
            message_count = sum(len(list(reader.messages(connections=[conn]))) for conn in reader.connections)
            
            # Get time range
            start_time = None
            end_time = None
            
            for conn in reader.connections:
                messages = list(reader.messages(connections=[conn]))
                if messages:
                    for _, timestamp, _ in messages:
                        if start_time is None or timestamp < start_time:
                            start_time = timestamp
                        if end_time is None or timestamp > end_time:
                            end_time = timestamp
            
            duration = (end_time - start_time) / 1e9 if start_time and end_time else 0.0
            
            return {
                "topics": topics,
                "message_count": message_count,
                "duration": duration,
                "start_time": start_time,
                "end_time": end_time
            }
    
    def get_topics(self) -> List[str]:
        """Get list of available topics"""
        return self.metadata.get('topics', [])
    
    def convert_to_parquet(self, output_path: str, topics: Optional[List[str]] = None) -> str:
        """Convert ROSbag to Parquet format"""
        output_path = Path(output_path)
        
        # Get data as DataFrame first
        df = self.convert_to_dataframe(topics)
        
        # Convert to PyArrow table and write to Parquet
        table = pa.Table.from_pandas(df)
        pq.write_table(table, output_path)
        
        return str(output_path)
    
    def convert_to_dataframe(self, topics: Optional[List[str]] = None) -> pd.DataFrame:
        """Convert ROSbag to Pandas DataFrame"""
        if not ROSBAGS_AVAILABLE:
            raise RuntimeError("rosbags library not available. Install with: pip install 'robolake-cli[rosbags]'")
        
        return self._convert_rosbags_to_dataframe(topics)
    
    def _convert_rosbags_to_dataframe(self, topics: Optional[List[str]] = None) -> pd.DataFrame:
        """Convert ROSbag to DataFrame using rosbags library"""
        if topics is None:
            topics = self.get_topics()
        
        data = []
        typestore = get_typestore(Stores.ROS2_FOXY)
        
        with AnyReader([self.bag_path], default_typestore=typestore) as reader:
            # Filter connections by topics if specified
            connections = reader.connections
            if topics:
                connections = [conn for conn in connections if conn.topic in topics]
            
            for connection, timestamp, rawdata in reader.messages(connections=connections):
                # Extract message data
                msg_data = self._extract_message_data(reader, connection, timestamp, rawdata)
                data.append(msg_data)
        
        return pd.DataFrame(data)
    
    def _extract_message_data(self, reader, connection, timestamp, rawdata) -> Dict[str, Any]:
        """Extract data from ROSbag message"""
        try:
            # Try to deserialize the message
            msg_obj = reader.deserialize(rawdata, connection.msgtype)
            
            # Extract basic metadata
            data = {
                "topic": connection.topic,
                "timestamp": timestamp / 1e9,  # Convert to seconds
                "msgtype": connection.msgtype
            }
            
            # Extract header timestamp if available
            if hasattr(msg_obj, 'header') and hasattr(msg_obj.header, 'stamp'):
                header_stamp = msg_obj.header.stamp
                if hasattr(header_stamp, 'sec') and hasattr(header_stamp, 'nanosec'):
                    data["header_timestamp"] = header_stamp.sec + header_stamp.nanosec / 1e9
            
            # Extract specific data based on message type
            if connection.msgtype == 'geometry_msgs/msg/PointStamped':
                if hasattr(msg_obj, 'point'):
                    point = msg_obj.point
                    data["x"] = point.x
                    data["y"] = point.y
                    data["z"] = point.z
            elif connection.msgtype == 'sensor_msgs/msg/Image':
                if hasattr(msg_obj, 'width') and hasattr(msg_obj, 'height'):
                    data["width"] = msg_obj.width
                    data["height"] = msg_obj.height
                    data["encoding"] = msg_obj.encoding
                    data["data_size"] = len(msg_obj.data) if hasattr(msg_obj, 'data') else 0
            elif connection.msgtype == 'sensor_msgs/msg/Imu':
                if hasattr(msg_obj, 'linear_acceleration'):
                    acc = msg_obj.linear_acceleration
                    data["accel_x"] = acc.x
                    data["accel_y"] = acc.y
                    data["accel_z"] = acc.z
                if hasattr(msg_obj, 'angular_velocity'):
                    gyro = msg_obj.angular_velocity
                    data["gyro_x"] = gyro.x
                    data["gyro_y"] = gyro.y
                    data["gyro_z"] = gyro.z
            
            return data
            
        except Exception as e:
            logging.warning(f"Error extracting message data: {e}")
            return {
                "topic": connection.topic,
                "timestamp": timestamp / 1e9,
                "error": str(e),
                "msgtype": connection.msgtype
            } 