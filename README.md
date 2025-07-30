# Chevin-Geotab Fleet Data Integration

A Python-based automated integration system that synchronizes fleet data between Geotab telematics platform and Chevin FleetWave fleet management system via SFTP file transfers.

## Overview

This integration service performs bidirectional data synchronization:
- **Geotab → Chevin**: Generates comprehensive fleet reports from Geotab data and uploads them to Chevin's SFTP server
- **Chevin → Geotab**: Downloads vehicle update files from Chevin and applies changes to Geotab devices

The system runs continuously with hourly automated execution, ensuring fleet data stays synchronized across both platforms.

## Features

### Report Generation
- **Asset Status Report**: Current vehicle status, locations, odometer readings, and driver assignments
- **Trips History Report**: Detailed trip logs with distances, durations, and route information
- **Exceptions Details Report**: Rule violations and exception events with location data
- **Engine Faults Report**: Diagnostic trouble codes and fault data from vehicle ECMs

### Data Synchronization
- **Real-time Location Services**: GPS coordinates converted to human-readable addresses
- **Zone Management**: Automatic zone detection using polygon geometry calculations
- **Unit Conversions**: Automatic conversion from metric (Geotab) to imperial units (miles, mph)
- **Timezone Handling**: UTC to Eastern Time conversion for all timestamps

### Vehicle Management
- **Bidirectional Updates**: Vehicle names, VINs, and group assignments synchronized between systems
- **Group Hierarchy**: Supports complex organizational structures with parent-child group relationships
- **Selective Updates**: Only modifies vehicles in designated changeable groups for data integrity

## Technical Architecture

### Core Components
- **MyGeotab API Client**: Authenticated connection to Geotab's REST API
- **SFTP File Transfer**: Secure file exchange with Chevin FleetWave servers
- **Data Caching System**: Optimized API usage through intelligent caching of reference data
- **Batch Processing**: Efficient bulk data retrieval and processing

### Performance Optimizations
- **Single API Calls**: Batch retrieval of all data types to minimize API overhead
- **Reference Data Caching**: Device, group, user, and zone data cached to avoid repeated lookups
- **Rate Limiting**: Built-in delays for address geocoding to respect API limits
- **Memory Management**: Efficient data structures for large fleet datasets

## Installation

### Prerequisites
```bash
# Python 3.7+
pip install mygeotab pandas xlwt paramiko pysftp pytz schedule
```

### Configuration
Update the configuration variables in the script:
```python
# Geotab API Credentials
GEO_USER = 'your_username'
GEO_DATABASE = 'your_database'
GEO_PASSWORD = 'your_password'

# SFTP Configuration
SFTP_CONFIG = {
    "host": "your_sftp_host",
    "username": "your_username", 
    "password": "your_password",
    "dir": "Import"
}
```

## Usage

### Automated Execution
```bash
python chevin.py
```
The script will:
1. Run an initial report generation
2. Schedule hourly execution
3. Continue running indefinitely

### Manual Execution
```python
from chevin import GeotabReportGenerator

generator = GeotabReportGenerator()
generator.generate_all_reports()
```

## Data Flow

### Report Generation Process
1. **Authentication**: Establish secure connection to Geotab API
2. **Data Caching**: Bulk retrieval of reference data (devices, groups, users, zones)
3. **Batch Processing**: Collect last hour's trip, exception, and fault data
4. **Report Generation**: Process data into four standardized report formats
5. **File Creation**: Generate Excel (.xls) files with formatted data
6. **SFTP Upload**: Transfer reports to Chevin FleetWave server

### Vehicle Synchronization Process
1. **File Download**: Retrieve vehicle update CSV from Chevin SFTP
2. **Data Parsing**: Extract vehicle changes (names, VINs, group assignments)
3. **Geotab Sync**: Apply updates to matching vehicles in Geotab database
4. **Validation**: Ensure updates only affect designated changeable groups

## Report Specifications

### Asset Status Report
- Device information and status
- Current location and zones
- Odometer and engine hours
- Driver assignments
- Communication status

### Trips History Report
- Start/stop times and locations
- Distance and duration metrics
- Speed and idling statistics
- Work vs. personal trip classification

### Exceptions Details Report
- Rule violations and events
- Location and duration data
- Driver and device associations
- Detailed exception descriptions

### Engine Faults Report
- Diagnostic trouble codes
- Controller and diagnostic information
- Fault timestamps and device data
- Driver assignments at fault time

## File Formats

### Generated Reports
- **Format**: Microsoft Excel (.xls)
- **Naming**: `washingtongas_{report_type}_{timestamp}.xls`
- **Encoding**: UTF-8 with proper Excel compatibility

### Vehicle Update CSV
```csv
Serial,ID,VIN,Name,Groups
G1234567890,device_id,1FTFW1ET5DFC12345,Vehicle Name,group1|group2
```

## Error Handling

- **API Failures**: Graceful degradation with retry mechanisms
- **SFTP Issues**: Connection timeout handling and reconnection
- **Data Validation**: Type checking and format validation
- **Logging**: Comprehensive error logging for troubleshooting

## Security Features

- **Credential Management**: Secure handling of API keys and passwords
- **SFTP Encryption**: All file transfers use SSH encryption
- **Data Validation**: Input sanitization and boundary checking
- **Access Control**: Group-based permissions for vehicle modifications

## Monitoring and Maintenance

### Logging
The system provides detailed console output for:
- API call results and data volumes
- File transfer status
- Update operations and results
- Error conditions and warnings

### Performance Metrics
- API call efficiency (batch vs. individual calls)
- Data processing volumes
- File transfer success rates
- Synchronization accuracy