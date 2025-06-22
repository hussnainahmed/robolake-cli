# RoboLake CLI

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Robotics data processing platform - Convert ROSbag files to analytics-ready formats**

RoboLake CLI is a powerful command-line tool that transforms ROSbag files into analytics-ready formats like Parquet, CSV, and JSON. It's designed for robotics engineers, researchers, and data scientists who need to analyze robotics data efficiently.

## 🚀 Features

- **📦 ROSbag Processing**: Read and convert ROS1/ROS2 bag files
- **🔄 Multiple Formats**: Export to Parquet, CSV, and JSON
- **🗄️ Local Data Catalog**: Store and query converted data with SQL
- **📊 Rich CLI**: Beautiful terminal interface with progress bars
- **🔍 Topic Filtering**: Extract specific topics from ROSbag files
- **⚡ Fast Processing**: Optimized for large robotics datasets

## 📦 Installation

### Prerequisites

- **Python**: 3.9 or higher
- **Git**: For cloning the repository

### Setup Virtual Environment (Recommended)

```bash
# Clone the repository
git clone https://github.com/hussnainahmed/robolake-cli.git
cd robolake-cli

# Create virtual environment
python -m venv .venv

# Activate virtual environment
# On Windows:
.venv\Scripts\activate
# On macOS/Linux:
source .venv/bin/activate

# Install the package
pip install -e .
```

### From Source (Development)

```bash
git clone https://github.com/hussnainahmed/robolake-cli.git
cd robolake-cli
pip install -e .
```

### With ROSbag Support

```bash
# Make sure virtual environment is activated
pip install -e ".[rosbags]"
```

### With Full Features

```bash
# Make sure virtual environment is activated
pip install -e ".[rosbags,iceberg]"
```

## 🎯 Quick Start

### 1. Analyze a ROSbag File

```bash
robolake info my_robot_data.bag
```

### 2. Convert to Parquet

```bash
robolake convert my_robot_data.bag --format parquet
```

### 3. Extract Specific Topics

```bash
robolake convert my_robot_data.bag --topics /imu/data,/gps/fix --format csv
```

### 4. Store in Data Catalog

```bash
# Initialize catalog
robolake init ./my_catalog

# Convert and store
robolake convert my_robot_data.bag --catalog ./my_catalog
```

### 5. Query Your Data

```bash
robolake query ./my_catalog "SELECT COUNT(*) FROM my_robot_data"
```

## 📖 Usage Examples

### Convert Position Data

```bash
# Convert position topic to Parquet
robolake convert robot_trajectory.bag --topics /leica/position --format parquet

# Output: robot_trajectory.parquet
```

### Analyze IMU Data

```bash
# Extract IMU data and query
robolake convert sensor_data.bag --topics /imu/data --catalog ./sensors
robolake query ./sensors "SELECT AVG(accel_x) FROM sensor_data WHERE accel_x > 0"
```

### Batch Processing

```bash
# Process multiple files
for file in *.bag; do
    robolake convert "$file" --catalog ./dataset
done
```

## 🛠️ Commands

### `robolake convert`

Convert ROSbag files to various formats.

```bash
robolake convert <input_file> [OPTIONS]

Options:
  --format [parquet|csv|json]  Output format (default: parquet)
  --topics TEXT                Comma-separated list of topics to extract
  --output, -o TEXT            Output file path
  --catalog TEXT               Data catalog path for storage
```

### `robolake info`

Show information about ROSbag files.

```bash
robolake info <input_file>
```

### `robolake init`

Initialize a new data catalog.

```bash
robolake init <catalog_path> [--force]
```

### `robolake query`

Execute SQL queries against data catalogs.

```bash
robolake query <catalog_path> <sql_query>
```

## 🗄️ Data Catalog

The data catalog provides local SQL querying capabilities:

```bash
# Initialize catalog
robolake init ./my_data

# Store ROSbag data
robolake convert robot_data.bag --catalog ./my_data

# Query with SQL
robolake query ./my_data "SELECT * FROM robot_data WHERE x > 0"
robolake query ./my_data "SELECT COUNT(*) as total_messages FROM robot_data"
robolake query ./my_data "SELECT MIN(x), MAX(x), AVG(y) FROM robot_data"
```

## ⚠️ Limitations

### Supported Message Types

**Currently Supported for Field Extraction:**
- `geometry_msgs/msg/PointStamped` - Extracts x, y, z coordinates
- `sensor_msgs/msg/Image` - Extracts width, height, encoding, data_size
- `sensor_msgs/msg/Imu` - Extracts acceleration and angular velocity

**Other Message Types:**
- All other message types are processed but only basic metadata is extracted
- Basic fields: `topic`, `timestamp`, `msgtype`, `header_timestamp` (if available)
- No custom field extraction for unsupported message types

### Current Limitations

1. **Limited Message Type Support**: Only 3 specific message types have custom field extraction
2. **No Generic Field Extraction**: Cannot automatically extract fields from unknown message types
3. **ROS2-Focused**: Uses ROS2_FOXY typestore by default (may not be optimal for ROS1 bags)
4. **No Apache Iceberg**: Iceberg functionality is declared but not implemented
5. **Local Storage Only**: Data catalog is local-only, no cloud sync capabilities
6. **Basic Error Handling**: Limited error recovery for corrupted or unsupported message types

### Known Issues

- **Large ROSbag Files**: May consume significant memory for very large files
- **Complex Message Types**: Nested or complex message structures may not extract properly
- **ROS1 Compatibility**: While supported, ROS1 bags may have limited field extraction
- **Image Data**: Only extracts metadata, not actual image pixel data

### Future Improvements

- [ ] Add support for more message types (Pose, Twist, LaserScan, etc.)
- [ ] Implement generic field extraction for unknown message types
- [ ] Add ROS1/ROS2 automatic detection
- [ ] Implement Apache Iceberg catalog functionality
- [ ] Add cloud sync capabilities
- [ ] Improve memory efficiency for large files
- [ ] Add support for image data extraction

## 🔧 Development

### Install in Development Mode

```bash
git clone https://github.com/hussnainahmed/robolake-cli.git
cd robolake-cli
pip install -e .
```

### Run Tests

```bash
pytest tests/
```

### Code Formatting

```bash
black src/
isort src/
```

## 📋 Requirements

- **Python**: 3.9 or higher
- **Dependencies**: pandas, pyarrow, duckdb, rich, click
- **Optional**: rosbags (for ROSbag processing), pyiceberg (for advanced catalog features)

## 🤝 Contributing

We welcome contributions! 

### Development Setup

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [rosbags](https://gitlab.com/ternaris/rosbags) - Pure Python ROSbag library
- [DuckDB](https://duckdb.org/) - Embedded analytical database
- [Rich](https://rich.readthedocs.io/) - Beautiful terminal output
- [Click](https://click.palletsprojects.com/) - Command line interface creation kit






