#!/usr/bin/env python3
"""
Test script for generic field extraction functionality
"""

import sys
from pathlib import Path

# Add the package to the path for testing
sys.path.insert(0, str(Path(__file__).parent / "src"))

from robolake_cli.processor import ROSbagProcessor

def test_generic_extraction():
    """Test the generic field extraction with a mock message"""
    
    # Create a mock ROS message-like object
    class MockPoint:
        __slots__ = ['x', 'y', 'z']
        def __init__(self):
            self.x = 1.0
            self.y = 2.0
            self.z = 3.0
    
    class MockHeader:
        __slots__ = ['stamp', 'frame_id']
        def __init__(self):
            self.stamp = MockTime()
            self.frame_id = "base_link"
    
    class MockTime:
        __slots__ = ['sec', 'nanosec']
        def __init__(self):
            self.sec = 1234567890
            self.nanosec = 123456789
    
    class MockPoseStamped:
        __slots__ = ['header', 'pose']
        def __init__(self):
            self.header = MockHeader()
            self.pose = MockPose()
    
    class MockPose:
        __slots__ = ['position', 'orientation']
        def __init__(self):
            self.position = MockPoint()
            self.orientation = MockQuaternion()
    
    class MockQuaternion:
        __slots__ = ['x', 'y', 'z', 'w']
        def __init__(self):
            self.x = 0.0
            self.y = 0.0
            self.z = 0.0
            self.w = 1.0
    
    # Test the flattening
    processor = ROSbagProcessor.__new__(ROSbagProcessor)  # Create instance without __init__
    
    mock_msg = MockPoseStamped()
    flattened = processor._flatten_ros_message(mock_msg)
    
    print("✅ Generic field extraction test:")
    print("Flattened fields:")
    for key, value in flattened.items():
        print(f"  {key}: {value}")
    
    # Check that we got the expected fields
    expected_fields = [
        'header.stamp.sec', 'header.stamp.nanosec', 'header.frame_id',
        'pose.position.x', 'pose.position.y', 'pose.position.z',
        'pose.orientation.x', 'pose.orientation.y', 'pose.orientation.z', 'pose.orientation.w'
    ]
    
    missing_fields = [field for field in expected_fields if field not in flattened]
    if missing_fields:
        print(f"❌ Missing fields: {missing_fields}")
        return False
    else:
        print("✅ All expected fields extracted successfully!")
        return True

if __name__ == "__main__":
    success = test_generic_extraction()
    sys.exit(0 if success else 1) 