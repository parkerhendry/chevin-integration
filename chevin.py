import mygeotab
import pandas as pd
from datetime import datetime, timedelta, time
import schedule
import time
from typing import Dict, List
import logging
import pytz
import xlwt
import paramiko
import io
import pysftp
import os

SFTP_CONFIG = {
    "host": "ftpus.chevinfleet.com",
    "port": 22,
    "username": "hidden",
    "password": "hidden",
    "dir": "Import"
}

GEO_USER = 'hidden'
GEO_DATABASE = 'hidden'
GEO_PASSWORD = 'hidden'

# Global API instance
geotab_api = None

class GeotabReportGenerator:
    def __init__(self):
        self.api = None
        self.devices_cache = {}
        self.groups_cache = {}
        self.users_cache = {}
        self.zones_cache = {}
        self.rules_cache = {}
        self.diagnostics_cache = {}
        self.controllers_cache = {}
        # Batch data caches
        self.trips_cache = []
        self.exceptions_cache = []
        self.fault_data_cache = []
        self.device_status_info_cache = []
        self.log_records_cache = []
        # StatusData caches - organized by device_id for quick lookup
        self.odometer_data_cache = {}  # device_id -> latest odometer data
        self.engine_hours_data_cache = {}  # device_id -> latest engine hours data
        
        # File configuration for sync
        self.remote_dir = "Export/geotab"
        self.remote_file = "fwgeotabinfo.csv"
        self.local_dir = "import"
        self.local_file = "fwgeotabinfo.csv"
        
        # Groups that can be changed
        self.groups_change = ['b2867', 'b27D4']
        
        # Ensure local directory exists
        os.makedirs(self.local_dir, exist_ok=True)
        
    def authenticate_geotab(self):
        """Authenticate with the MyGeotab API."""
        try:
            self.api = mygeotab.API(username=GEO_USER, password=GEO_PASSWORD, database=GEO_DATABASE)
            self.api.authenticate()
            print("Authenticated with Geotab successfully.")
            return True
        except Exception as e:
            print(f"Failed to authenticate with Geotab: {e}")
            return False
    
    def get_geotab_data(self, type_name: str, **kwargs) -> List[Dict]:
        """Get data from Geotab API with error handling."""
        try:
            if not self.api:
                if not self.authenticate_geotab():
                    return []
            
            print(f"Calling Geotab API for type: {type_name}")
            result = self.api.call("Get", typeName=type_name, **kwargs)
            print(f"Retrieved {len(result)} records for {type_name}")
            return result
        except Exception as e:
            print(f"Error getting {type_name} data: {e}")
            return []
    
    def get_addresses(self, coordinates: List[Dict]) -> List[Dict]:
        """Get addresses from coordinates using GetAddresses API with rate limiting."""
        try:
            if not coordinates:
                return []
            
            # Add rate limiting - wait 0.15 seconds between address API calls
            # This ensures we stay under 450 calls per minute (6.67 calls per second max)
            time.sleep(0.15)
            
            result = self.api.call('GetAddresses', coordinates=coordinates)
            return result
        except Exception as e:
            print(f"Error getting addresses: {e}")
            return []
    
    def cache_reference_data(self):
        """Cache reference data to avoid repeated API calls."""
        print("Caching reference data...")
        
        # Cache devices
        devices = self.get_geotab_data("Device")
        self.devices_cache = {device['id']: device for device in devices}
        
        # Cache groups
        groups = self.get_geotab_data("Group")
        self.groups_cache = {group['id']: group for group in groups}
        
        # Cache users
        users = self.get_geotab_data("User")
        self.users_cache = {user['id']: user for user in users}
        
        # Cache zones
        zones = self.get_geotab_data("Zone")
        self.zones_cache = {zone['id']: zone for zone in zones}
        
        # Cache rules
        rules = self.get_geotab_data("Rule")
        self.rules_cache = {rule['id']: rule for rule in rules}
        
        # Cache diagnostics
        diagnostics = self.get_geotab_data("Diagnostic")
        self.diagnostics_cache = {diag['id']: diag for diag in diagnostics}
        
        # Cache controllers
        controllers = self.get_geotab_data("Controller")
        self.controllers_cache = {controller['id']: controller for controller in controllers}
        
        print("Reference data cached successfully.")
    
    def cache_batch_data(self):
        """Cache all batch data for the last hour in single calls."""
        print("Caching batch data for last hour...")
        
        search_criteria = self.get_last_hour_search()
        
        # Cache all trips from last hour - SINGLE CALL
        self.trips_cache = self.get_geotab_data("Trip", search=search_criteria)
        print(f"Cached {len(self.trips_cache)} trips")
        
        # Cache all exceptions from last hour - SINGLE CALL
        self.exceptions_cache = self.get_geotab_data("ExceptionEvent", search=search_criteria)
        print(f"Cached {len(self.exceptions_cache)} exceptions")
        
        # Cache all fault data from last hour - SINGLE CALL
        self.fault_data_cache = self.get_geotab_data("FaultData", search=search_criteria)
        print(f"Cached {len(self.fault_data_cache)} fault data")
        
        # Cache device status info - SINGLE CALL
        self.device_status_info_cache = self.get_geotab_data("DeviceStatusInfo", search=search_criteria)
        print(f"Cached {len(self.device_status_info_cache)} device status info")
        
        # Cache log records for last hour - SINGLE CALL
        self.log_records_cache = self.get_geotab_data("LogRecord", search=search_criteria)
        print(f"Cached {len(self.log_records_cache)} log records")
        
        # Cache ALL odometer data for last hour - SINGLE CALL
        odometer_search = {
            "diagnosticSearch": {"id": "DiagnosticRawOdometerId"},
            **search_criteria
        }
        odometer_data = self.get_geotab_data("StatusData", search=odometer_search)
        # Organize by device_id, keeping the latest reading per device
        for data in odometer_data:
            device_id = data.get('device', {}).get('id', '')
            if device_id:
                # Keep the latest reading for each device
                if device_id not in self.odometer_data_cache or data.get('dateTime', '') > self.odometer_data_cache[device_id].get('dateTime', ''):
                    self.odometer_data_cache[device_id] = data
        print(f"Cached odometer data for {len(self.odometer_data_cache)} devices")
        
        # Cache ALL engine hours data for last hour - SINGLE CALL
        engine_hours_search = {
            "diagnosticSearch": {"id": "DiagnosticEngineHoursId"},
            **search_criteria
        }
        engine_hours_data = self.get_geotab_data("StatusData", search=engine_hours_search)
        # Organize by device_id, keeping the latest reading per device
        for data in engine_hours_data:
            device_id = data.get('device', {}).get('id', '')
            if device_id:
                # Keep the latest reading for each device
                if device_id not in self.engine_hours_data_cache or data.get('dateTime', '') > self.engine_hours_data_cache[device_id].get('dateTime', ''):
                    self.engine_hours_data_cache[device_id] = data
        print(f"Cached engine hours data for {len(self.engine_hours_data_cache)} devices")
    
    def get_last_hour_search(self) -> Dict:
        """Create search criteria for last hour."""
        now = datetime.utcnow()
        one_hour_ago = now - timedelta(hours=1)
        
        return {
            "fromDate": one_hour_ago.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "toDate": now.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        }
    
    def get_device_name(self, device_id: str) -> str:
        """Get device name from cache."""
        device = self.devices_cache.get(device_id, {})
        return device.get('name', '')
    
    def get_device_groups(self, device_id: str) -> str:
        """Get device group names from cache."""
        device = self.devices_cache.get(device_id, {})
        groups = device.get('groups', [])
        group_names = []
        for group in groups:
            group_id = group.get('id') if isinstance(group, dict) else group
            group_data = self.groups_cache.get(group_id, {})
            if group_data.get('name'):
                group_names.append(group_data['name'])
        return ', '.join(group_names)
    
    def get_user_info(self, user_id: str) -> tuple:
        """Get user serial number and employee number from cache."""
        user = self.users_cache.get(user_id, {})
        keys = user.get('keys', [])
        serial_number = keys[0].get('serialNumber', '') if keys else ''
        employee_number = user.get('employeeNo', '')
        return serial_number, employee_number
    
    def get_zone_names(self, zone_ids: List[str]) -> str:
        """Get zone names from cache."""
        zone_names = []
        for zone_id in zone_ids:
            zone = self.zones_cache.get(zone_id, {})
            if zone.get('name'):
                zone_names.append(zone['name'])
        return ', '.join(zone_names)
    
    def point_in_polygon(self, point_lat: float, point_lon: float, polygon_points: List[Dict]) -> bool:
        """
        Check if a point is inside a polygon using ray casting algorithm.
        polygon_points should be a list of dictionaries with 'x' (longitude) and 'y' (latitude) keys.
        """
        if not polygon_points or len(polygon_points) < 3:
            return False
        
        x, y = point_lon, point_lat
        n = len(polygon_points)
        inside = False
        
        p1x, p1y = polygon_points[0]['x'], polygon_points[0]['y']
        for i in range(1, n + 1):
            p2x, p2y = polygon_points[i % n]['x'], polygon_points[i % n]['y']
            if y > min(p1y, p2y):
                if y <= max(p1y, p2y):
                    if x <= max(p1x, p2x):
                        if p1y != p2y:
                            xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                        if p1x == p2x or x <= xinters:
                            inside = not inside
            p1x, p1y = p2x, p2y
        
        return inside
    
    def get_zones_for_location(self, latitude: float, longitude: float) -> str:
        """Get zone names that contain the given coordinates."""
        if not latitude or not longitude:
            return ''
        
        matching_zones = []
        
        for zone_id, zone in self.zones_cache.items():
            # Skip zones without points
            if not zone.get('points'):
                continue
            
            # Check if point is in this zone
            if self.point_in_polygon(latitude, longitude, zone['points']):
                zone_name = zone.get('name', '')
                if zone_name:
                    matching_zones.append(zone_name)
        
        return ', '.join(matching_zones)
    
    def get_driver_from_device_status(self, device_id: str) -> str:
        """Get driver ID from device status info cache."""
        for status in self.device_status_info_cache:
            status_device_dict = status.get('device', {})
            if isinstance(status_device_dict, str):
                status_device_id = status_device_dict
            else:
                status_device_id = status_device_dict.get('id', '') if status_device_dict else ''
            
            if status_device_id == device_id:
                driver_dict = status.get('driver', {})
                if isinstance(driver_dict, str):
                    return driver_dict
                else:
                    return driver_dict.get('id', '') if driver_dict else ''
        
        return ''
    
    def format_datetime(self, dt_input) -> tuple:
        """Format datetime string or datetime object into separate date and time in Eastern time."""
        try:
            if not dt_input or dt_input == '' or dt_input is None:
                return '', ''
            
            # Check if it's already a datetime object
            if isinstance(dt_input, datetime):
                dt = dt_input
            else:
                # It's a string, so parse it
                dt_str = str(dt_input)  # Convert to string just in case
                
                # Handle different datetime formats from Geotab API
                if dt_str.endswith('Z'):
                    # Remove the Z and add explicit UTC offset
                    dt_str = dt_str[:-1] + '+00:00'
                elif not dt_str.endswith('+00:00') and 'T' in dt_str and '+' not in dt_str and '-' not in dt_str[-6:]:
                    # If no timezone info, assume UTC
                    dt_str = dt_str + '+00:00'
                
                # Parse the datetime string
                dt = datetime.fromisoformat(dt_str)
            
            # Convert to Eastern time
            eastern = pytz.timezone('US/Eastern')
            
            # If datetime is naive (no timezone), assume UTC
            if dt.tzinfo is None:
                utc = pytz.UTC
                dt = utc.localize(dt)
            
            # Convert to Eastern time
            dt_eastern = dt.astimezone(eastern)
            
            # Format as MM/DD/YYYY and 12-hour time
            date_str = dt_eastern.strftime('%m/%d/%Y')
            time_str = dt_eastern.strftime('%I:%M:%S %p')
            
            return date_str, time_str
            
        except Exception as e:
            # Log the error for debugging
            logging.warning(f"Error parsing datetime '{dt_input}' (type: {type(dt_input)}): {e}")
            return '', ''
    
    def format_duration(self, duration_input) -> str:
        """Format various duration formats into HH:MM:SS format."""
        try:
            if not duration_input or duration_input == '' or duration_input is None:
                return ''
            
            # Handle datetime.time objects directly
            if hasattr(duration_input, 'hour') and hasattr(duration_input, 'minute') and hasattr(duration_input, 'second'):
                return f"{duration_input.hour:02d}:{duration_input.minute:02d}:{duration_input.second:02d}"
            
            duration_str = str(duration_input).strip()
            
            # If it's a number (milliseconds), convert to time format
            if duration_str.replace('.', '').isdigit():
                total_seconds = int(float(duration_str) / 1000)
                hours = total_seconds // 3600
                minutes = (total_seconds % 3600) // 60
                seconds = total_seconds % 60
                return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            
            # Remove any quotes that might be present
            duration_str = duration_str.strip('"\'')
            
            total_seconds = 0
            
            # Handle format with days (e.g., "6.18:10:19.3440000")
            if '.' in duration_str and ':' in duration_str:
                # Find the first colon to split days from time
                first_colon = duration_str.find(':')
                dot_before_colon = duration_str.rfind('.', 0, first_colon)
                
                if dot_before_colon != -1:
                    days_part = duration_str[:dot_before_colon]
                    time_part = duration_str[dot_before_colon + 1:]
                    
                    try:
                        days = float(days_part)
                        total_seconds += int(days * 24 * 3600)  # Convert days to seconds
                        duration_str = time_part
                    except ValueError:
                        # If days part can't be parsed, continue with original string
                        pass
            
            # Handle standard time format (HH:MM:SS or HH:MM:SS.ffffff)
            if ':' in duration_str:
                # Split by colon
                time_parts = duration_str.split(':')
                
                if len(time_parts) >= 2:
                    hours = int(time_parts[0])
                    minutes = int(time_parts[1])
                    
                    total_seconds += hours * 3600 + minutes * 60
                    
                    # Handle seconds if present
                    if len(time_parts) >= 3:
                        # Handle seconds with possible decimal part
                        seconds_str = time_parts[2]
                        seconds = int(float(seconds_str))
                        total_seconds += seconds
            
            # Convert total seconds back to HH:MM:SS format
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            
        except Exception as e:
            logging.warning(f"Error formatting duration '{duration_input}' (type: {type(duration_input)}): {e}")
            return ''
    
    def km_to_miles(self, km: float) -> float:
        """Convert kilometers to miles."""
        if km is None or km == '':
            return 0
        try:
            return float(km) * 0.621371
        except (ValueError, TypeError):
            return 0
    
    def ms_to_minutes(self, ms) -> str:
        """Convert milliseconds to HH:MM:SS format."""
        return self.format_duration(ms)
    
    def kmh_to_mph(self, kmh: float) -> float:
        """Convert km/h to mph."""
        return kmh * 0.621371 if kmh else 0

    def get_most_recent_trip_from_cache(self, device_id: str) -> dict:
        """Get the most recent trip for a device from cached trips data."""
        # Filter trips for this device
        device_trips = [trip for trip in self.trips_cache 
                    if trip.get('device', {}).get('id', '') == device_id]
        
        if not device_trips:
            return {}
        
        # Sort by stop time (most recent first) and return the first one
        device_trips.sort(key=lambda x: x.get('stop', ''), reverse=True)
        return device_trips[0]
    
    def meters_to_miles(self, meters: float) -> float:
        """Convert meters to miles."""
        if meters is None or meters == '':
            return 0
        try:
            return float(meters) * 0.000621371
        except (ValueError, TypeError):
            return 0

    def get_current_odometer(self, device_id: str) -> float:
        """Get current odometer reading for device from cached data, converted to miles."""
        # The cache_batch_data() method already stores the latest reading per device
        # by comparing dateTime values, so we can directly use the cached data
        data = self.odometer_data_cache.get(device_id, {})
        odometer_meters = data.get('data', 0) if data else 0
        return self.meters_to_miles(odometer_meters)

    def get_current_engine_hours(self, device_id: str) -> float:
        """Get current engine hours for device from cached data."""
        # The cache_batch_data() method already stores the latest reading per device
        # by comparing dateTime values, so we can directly use the cached data
        data = self.engine_hours_data_cache.get(device_id, {})
        return data.get('data', 0) if data else 0
    
    def get_trip_odometer_at_start(self, device_id: str, trip_start: str) -> float:
        """Get odometer reading at trip start from cached odometer data, converted to miles."""
        # Use the cached odometer data - return the latest reading we have
        return self.get_current_odometer(device_id)
    
    def generate_asset_status_report(self) -> pd.DataFrame:
        """Generate Asset Status Report using cached data."""
        print("Generating Asset Status Report...")
        
        # Use cached device status info
        device_status_info = self.device_status_info_cache
        
        report_data = []
        
        for status in device_status_info:
            device_dict = status.get('device', {})
            if isinstance(device_dict, str):
                device_id = device_dict
            else:
                device_id = device_dict.get('id', '') if device_dict else ''
                
            device = self.devices_cache.get(device_id, {})
            
            # Get current odometer and engine hours from cached data
            current_odometer = self.get_current_odometer(device_id)
            current_engine_hours = self.get_current_engine_hours(device_id)
            
            # Convert 0 values to empty strings for odometer and engine hours
            display_odometer = current_odometer if current_odometer != 0 else ''
            display_engine_hours = current_engine_hours if current_engine_hours != 0 else ''
            
            # Get location and zones
            location = ''
            location_zones = ''
            if status.get('longitude') and status.get('latitude'):
                coordinates = [{"x": status['longitude'], "y": status['latitude']}]
                addresses = self.get_addresses(coordinates)
                location = addresses[0].get('formattedAddress', '') if addresses else ''
                
                # Get zones for this location
                location_zones = self.get_zones_for_location(status['latitude'], status['longitude'])
            
            # Get last trip from cached data
            last_trip = self.get_most_recent_trip_from_cache(device_id)
            last_trip_date, last_trip_time = '', ''
            
            if last_trip:
                start_datetime = last_trip.get('start', '')
                last_trip_date, last_trip_time = self.format_datetime(start_datetime)
            
            # Get driver info
            driver_dict = status.get('driver', {})
            if isinstance(driver_dict, str):
                driver_id = driver_dict
            else:
                driver_id = driver_dict.get('id', '') if driver_dict else ''
            driver_serial, driver_employee = self.get_user_info(driver_id)
            
            # Format dates and times
            active_from_date, active_from_time = self.format_datetime(device.get('activeFrom', ''))
            active_to_date, active_to_time = self.format_datetime(device.get('activeTo', ''))
            last_gps_date, last_gps_time = self.format_datetime(status.get('dateTime', ''))
            
            row = {
                'DeviceName': device.get('name', ''),
                'DeviceVIN': device.get('vehicleIdentificationNumber', ''),
                'DevicePlan': device.get('devicePlans', [''])[0] if device.get('devicePlans') else '',
                'CurrentOdometer': display_odometer,
                'DeviceGroup': self.get_device_groups(device_id),
                'DrivingState': 'Driving' if status.get('isDriving', False) else 'Stopped',
                'Location': location,
                'LocationZones': location_zones,
                'CurrentEngineHours': display_engine_hours,
                'ActiveFromDate': active_from_date,
                'ActiveFromTime': active_from_time,
                'ActiveToDate': active_to_date,
                'ActiveToTime': active_to_time,
                'SerialNumber': device.get('serialNumber', ''),
                'DeviceId': device_id,
                'IsCommunicating': 'OK' if status.get('isDeviceCommunicating', False) else 'Device is not downloading data',
                'LastTripDate': last_trip_date,
                'LastTripTime': last_trip_time,
                'LastGpsDate': last_gps_date,
                'LastGpsTime': last_gps_time,
                'DriverSerialNumber': driver_serial,
                'DriverEmployeeNumber': driver_employee,
                'DeviceSerialNumber': device.get('serialNumber', ''),
                'EngineVIN': device.get('engineVehicleIdentificationNumber', ''),
                'VINMatch': device.get('vehicleIdentificationNumber', '') == device.get('engineVehicleIdentificationNumber', '')
            }
            
            report_data.append(row)

        print(f"Asset Status Report: Processed {len(report_data)} devices")
        
        return pd.DataFrame(report_data)

    def generate_trips_history_report(self) -> pd.DataFrame:
        """Generate Trips History Report with distance conversion to miles."""
        print("Generating Trips History Report...")
        
        # Use cached trips data
        trips = self.trips_cache
        
        report_data = []
        
        for trip in trips:
            device_id = trip.get('device', {}).get('id', '')
            
            # Handle driver field
            driver_dict = trip.get('driver', {})
            driver_id = ''
            if isinstance(driver_dict, dict):
                driver_id = driver_dict.get('id', '')
            
            # Get coordinates for location lookup
            location = ''
            location_zones = ''
            stop_point = trip.get('stopPoint', {})
            if stop_point.get('x') and stop_point.get('y'):
                coordinates = [{"x": stop_point['x'], "y": stop_point['y']}]
                addresses = self.get_addresses(coordinates)
                location = addresses[0].get('formattedAddress', '') if addresses else ''
                
                # Get zones for this location
                location_zones = self.get_zones_for_location(stop_point['y'], stop_point['x'])
            
            # Get odometer at start from cached data
            odometer_at_start = self.get_trip_odometer_at_start(device_id, trip.get('start', ''))
            
            # Get driver info only if we have a valid driver_id
            driver_serial, driver_employee = '', ''
            if driver_id:
                driver_serial, driver_employee = self.get_user_info(driver_id)
            
            # Format dates and times
            start_date, start_time = self.format_datetime(trip.get('start', ''))
            stop_date, stop_time = self.format_datetime(trip.get('stop', ''))
            
            row = {
                'DeviceName': self.get_device_name(device_id),
                'DeviceId': device_id,
                'DeviceIdHex': device_id,
                'DeviceGroup': self.get_device_groups(device_id),
                'StartDate': start_date,
                'StartTime': start_time,
                'DrivingDuration': self.ms_to_minutes(trip.get('drivingDuration', 0)),
                'StopDate': stop_date,
                'StopTime': stop_time,
                'Distance': self.km_to_miles(trip.get('distance', 0)),  # Convert to miles
                'StopDuration': self.ms_to_minutes(trip.get('stopDuration', 0)),
                'Latitude': stop_point.get('y', 0),
                'Longitude': stop_point.get('x', 0),
                'Location': location,
                'LocationZones': location_zones,
                'IdlingDuration': self.ms_to_minutes(trip.get('idlingDuration', 0)),
                'MaximumSpeed': self.kmh_to_mph(trip.get('maximumSpeed', 0)),
                'IsStartWork': 1 if not trip.get('afterHoursStart', True) else 0,
                'IsStopWork': 1 if not trip.get('afterHoursEnd', True) else 0,
                'WorkDistance': self.km_to_miles(trip.get('workDistance', 0)),  # Convert to miles
                'WorkTripTime': self.ms_to_minutes(trip.get('workDrivingDuration', 0)),
                'WorkStopTime': self.ms_to_minutes(trip.get('workStopDuration', 0)),
                'OdometerAtStart': odometer_at_start,
                'DriverSerialNumber': driver_serial,
                'DriverEmployeeNumber': driver_employee,
                'DeviceSerialNumber': self.devices_cache.get(device_id, {}).get('serialNumber', '')
            }
            
            report_data.append(row)

        print(f"Trips History Report: Processed {len(report_data)} trips")
        
        return pd.DataFrame(report_data)

    def generate_exceptions_report(self) -> pd.DataFrame:
        """Generate Exceptions Details Report with distance conversion to miles."""
        print("Generating Exceptions Details Report...")
        
        # Use cached exception events
        exceptions = self.exceptions_cache
        
        report_data = []
        
        for exception in exceptions:
            device_id = exception.get('device', {}).get('id', '')
            rule_id = exception.get('rule', {}).get('id', '')
            driver_dict = exception.get('driver', {})
            driver_id = ''
            if isinstance(driver_dict, dict):
                driver_id = driver_dict.get('id', '')
            
            # Get location from cached LogRecords
            longitude, latitude, location, location_zones = 0, 0, '', ''
            
            # Find matching log records for this device and time
            exception_time = exception.get('activeFrom', '')
            matching_logs = [log for log in self.log_records_cache 
                        if log.get('device', {}).get('id') == device_id 
                        and log.get('dateTime', '') >= exception_time]
            
            if matching_logs:
                # Sort by time and take the closest one
                matching_logs.sort(key=lambda x: x.get('dateTime', ''))
                first_record = matching_logs[0]
                longitude = first_record.get('longitude', 0)
                latitude = first_record.get('latitude', 0)
                
                if longitude and latitude:
                    coordinates = [{"x": longitude, "y": latitude}]
                    addresses = self.get_addresses(coordinates)
                    location = addresses[0].get('formattedAddress', '') if addresses else ''
                    
                    # Get zones for this location
                    location_zones = self.get_zones_for_location(latitude, longitude)
            
            # Get rule info from cache
            rule = self.rules_cache.get(rule_id, {})
            
            # Get driver info
            driver_serial, driver_employee = self.get_user_info(driver_id)
            
            # Format dates and times
            start_date, start_time = self.format_datetime(exception.get('activeFrom', ''))
            
            row = {
                'DeviceName': self.get_device_name(device_id),
                'DeviceIdHex': device_id,
                'DeviceId': device_id,
                'DeviceGroup': self.get_device_groups(device_id),
                'RuleName': rule.get('name', ''),
                'Longitude': longitude,
                'Latitude': latitude,
                'Location': location,
                'LocationZones': location_zones,
                'StartDate': start_date,
                'StartTime': start_time,
                'Duration': self.ms_to_minutes(exception.get('duration', 0)),
                'Distance': self.km_to_miles(exception.get('distance', 0)),  # Convert to miles
                'ExtraInfo': '',
                'Details': rule.get('comment', ''),
                'DriverSerialNumber': driver_serial,
                'DriverEmployeeNumber': driver_employee,
                'DeviceSerialNumber': self.devices_cache.get(device_id, {}).get('serialNumber', '')
            }
            
            report_data.append(row)

        print(f"Exceptions Report: Processed {len(report_data)} exceptions")
        
        return pd.DataFrame(report_data)

    def generate_engine_faults_report(self) -> pd.DataFrame:
        """Generate Engine Faults Report."""
        print("Generating Engine Faults Report...")
        
        # Use cached fault data
        faults = self.fault_data_cache
        
        report_data = []
        
        for fault in faults:
            device_id = fault.get('device', {}).get('id', '')
            diagnostic_id = fault.get('diagnostic', {}).get('id', '')
            controller_id = fault.get('controller', {}).get('id', '')
            
            # Get driver from device status since FaultData doesn't contain driver info
            driver_id = self.get_driver_from_device_status(device_id)
            
            # Get diagnostic info from cache
            diagnostic = self.diagnostics_cache.get(diagnostic_id, {})
            
            # Get controller info from cache
            controller = self.controllers_cache.get(controller_id, {})
            controller_name = controller.get('name', '')
            
            # Get driver info
            driver_serial, driver_employee = self.get_user_info(driver_id)
            
            # Format date and time
            fault_date, fault_time = self.format_datetime(fault.get('dateTime', ''))
            
            row = {
                'DeviceName': self.get_device_name(device_id),
                'DeviceId': device_id,
                'DeviceIdHex': device_id,
                'DeviceGroup': self.get_device_groups(device_id),
                'Date': fault_date,
                'Time': fault_time,
                'DiagnosticName': diagnostic.get('name', ''),
                'SourceName': diagnostic.get('source', ''),
                'ControllerName': controller_name,
                'DiagnosticCode': diagnostic.get('code', ''),
                'DriverSerialNumber': driver_serial,
                'DriverEmployeeNumber': driver_employee,
                'DeviceSerialNumber': self.devices_cache.get(device_id, {}).get('serialNumber', '')
            }
            
            report_data.append(row)

        print("Faults found: ", len(report_data))
        
        return pd.DataFrame(report_data)
    
    def save_df_to_xls(self, df, filename):
        workbook = xlwt.Workbook()
        sheet = workbook.add_sheet('Sheet1')

        # Write column headers
        for col_index, col_name in enumerate(df.columns):
            sheet.write(0, col_index, col_name)

        # Write row data
        for row_index, row in df.iterrows():
            for col_index, value in enumerate(row):
                sheet.write(row_index + 1, col_index, value)

        workbook.save(filename)
    
    def save_reports_to_excel(self, reports: Dict[str, pd.DataFrame]):
        """Save all reports to .xls files using xlwt directly."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        for report_name, df in reports.items():
            # Convert report name to lowercase and replace underscores with spaces
            formatted_name = report_name.lower().replace('_', ' ')
            filename = f"washingtongas_{formatted_name}_{timestamp}.xls"

    def save_reports_to_sftp(self, reports: Dict[str, pd.DataFrame]):
        """Save all reports to .xls files and upload to SFTP server."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create SFTP connection
        try:
            transport = paramiko.Transport((SFTP_CONFIG["host"], SFTP_CONFIG["port"]))
            transport.connect(username=SFTP_CONFIG["username"], password=SFTP_CONFIG["password"])
            sftp = paramiko.SFTPClient.from_transport(transport)
            
            # Change to the target directory
            try:
                sftp.chdir(SFTP_CONFIG["dir"])
            except FileNotFoundError:
                logging.warning(f"Directory {SFTP_CONFIG['dir']} not found, using root directory")
            
            print(f"Connected to SFTP server: {SFTP_CONFIG['host']}")
            
            for report_name, df in reports.items():
                formatted_name = report_name.lower().replace('_', ' ')
                filename = f"washingtongas_{formatted_name}_{timestamp}.xls"
                try:
                    # Create XLS file in memory
                    xls_buffer = self.create_xls_buffer(df)
                    
                    # Upload to SFTP server
                    sftp.putfo(xls_buffer, filename)
                    print(f"Uploaded {report_name} to SFTP server as {filename}")
                    
                except Exception as e:
                    print(f"Error uploading {report_name} to SFTP: {e}")
            
            # Close SFTP connection
            sftp.close()
            transport.close()
            print("SFTP connection closed")
            
        except Exception as e:
            print(f"Error connecting to SFTP server: {e}")

    def create_xls_buffer(self, df):
        """Create XLS file in memory buffer."""
        buffer = io.BytesIO()
        
        # Create workbook in memory
        workbook = xlwt.Workbook()
        sheet = workbook.add_sheet('Sheet1')
        
        # Write column headers
        for col_index, col_name in enumerate(df.columns):
            sheet.write(0, col_index, col_name)
        
        # Write row data
        for row_index, row in df.iterrows():
            for col_index, value in enumerate(row):
                sheet.write(row_index + 1, col_index, value)
        
        # Save to buffer
        workbook.save(buffer)
        buffer.seek(0)  # Reset buffer position to beginning
        
        return buffer
    
    def get_groups_change(self, group_id: str, groups: dict, groups_change: list = None) -> list:
        """
        Recursively get all child groups that can be changed
        
        Args:
            group_id: The group ID to start from
            groups: Dictionary of all groups
            groups_change: List to accumulate changeable groups
            
        Returns:
            List of group IDs that can be changed
        """
        if groups_change is None:
            groups_change = []
            
        group = groups.get(group_id, {})
        children = group.get('children', [])
        
        for child in children:
            child_id = child.get('id')
            if child_id:
                groups_change.append(child_id)
                groups_change = self.get_groups_change(child_id, groups, groups_change)
        
        return groups_change

    def download_sftp_file(self) -> bool:
        """
        Download the CSV file from SFTP server
        
        Returns:
            True if successful, False otherwise
        """
        try:
            remote_path = f"{self.remote_dir}/{self.remote_file}"
            local_path = f"{self.local_dir}/{self.local_file}"
            
            print(f"Connecting to SFTP server: {SFTP_CONFIG['host']}")
            with pysftp.Connection(
                host=SFTP_CONFIG["host"],
                username=SFTP_CONFIG["username"],
                password=SFTP_CONFIG["password"],
                port=SFTP_CONFIG["port"]
            ) as sftp:
                # Check if remote directory exists and has files
                try:
                    files_in_dir = sftp.listdir(self.remote_dir)
                    print(f"Files in remote directory: {len(files_in_dir)}")
                    
                    if self.remote_file not in files_in_dir:
                        logging.warning(f"Remote file {self.remote_file} not found in {self.remote_dir}")
                        return False
                    
                    # Download the file
                    sftp.get(remote_path, local_path, preserve_mtime=True)
                    print(f"Successfully downloaded {remote_path} to {local_path}")
                    return True
                    
                except Exception as e:
                    print(f"Error accessing remote directory or file: {e}")
                    return False
                    
        except Exception as e:
            print(f"SFTP connection error: {e}")
            return False

    def parse_csv_updates(self) -> list:
        """
        Parse the downloaded CSV file and extract vehicle updates
        
        Returns:
            List of vehicle update dictionaries
        """
        local_path = f"{self.local_dir}/{self.local_file}"
        
        if not os.path.exists(local_path):
            logging.warning(f"Local file {local_path} does not exist")
            return []
        
        updates = []
        try:
            with open(local_path, 'r', newline='', encoding='utf-8') as csvfile:
                # Read all lines first to handle potential header
                lines = csvfile.readlines()
                
                if not lines:
                    logging.warning("CSV file is empty")
                    return []
                
                # Check if first line is a header
                first_line = lines[0].strip()
                if len(first_line) > 12 and (not first_line.startswith('G') or first_line[12] != ','):
                    lines = lines[1:]  # Skip header
                    print("Skipped header row")
                
                print(f"Processing {len(lines)} data lines")
                
                # Process each line
                for line_num, line in enumerate(lines, 1):
                    try:
                        tokens = line.strip().replace('\r', '').replace('\n', '').split(',')
                        
                        if len(tokens) == 5:
                            update = {
                                'serial': tokens[0].strip(),
                                'id': tokens[1].strip(),
                                'vin': tokens[2].strip(),
                                'name': tokens[3].strip(),
                                'groups': [g.strip() for g in tokens[4].split('|') if g.strip()]
                            }
                            print(f"Update parsed: {update}")
                            updates.append(update)
                        else:
                            logging.warning(f"Line {line_num}: Invalid format, expected 5 columns, got {len(tokens)}")
                            
                    except Exception as e:
                        print(f"Error parsing line {line_num}: {e}")
                        continue
                        
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return []
        
        print(f"Parsed {len(updates)} vehicle updates from CSV")
        return updates

    def sync_vehicles_with_geotab(self, updates: list):
        """
        Sync vehicle updates with Geotab
        
        Args:
            updates: List of vehicle update dictionaries
        """
        if not updates:
            print("No updates to process")
            return
        
        try:
            # Get current devices from Geotab (use cached data if available)
            devices = list(self.devices_cache.values()) if self.devices_cache else self.get_geotab_data("Device")
            print(f"Retrieved {len(devices)} devices from Geotab")
            
            # Get groups from Geotab (use cached data if available)
            groups_dict = self.groups_cache if self.groups_cache else {group['id']: group for group in self.get_geotab_data("Group")}
            
            # Build list of changeable groups
            changeable_groups = []
            for group_id in self.groups_change:
                changeable_groups = self.get_groups_change(group_id, groups_dict, changeable_groups)
            
            print(f"Changeable groups: {len(changeable_groups)}")
            
            # Process updates
            api_calls = []
            devices_to_update = []
            
            for update in updates:
                try:
                    # Find matching device by serial number and VIN
                    matching_device = None
                    for device in devices:
                        if (device['serialNumber'] == update['serial'] and 
                            device.get('engineVehicleIdentificationNumber', '') == update['vin']):
                            matching_device = device
                            break
                    
                    if not matching_device:
                        logging.warning(f"No matching device found for serial: {update['serial']}, VIN: {update['vin']}")
                        continue
                    
                    # Check if updates are needed
                    needs_update = False
                    updated_device = dict(matching_device)
                    
                    # Check name update
                    new_name = update['name'] if update['name'] else update['serial']
                    if updated_device['name'] != new_name:
                        updated_device['name'] = new_name
                        needs_update = True
                        print(f"Name update needed for {update['serial']}: '{matching_device['name']}' -> '{new_name}'")
                    
                    # Check VIN update
                    if updated_device.get('vehicleIdentificationNumber', '') != update['vin']:
                        updated_device['vehicleIdentificationNumber'] = update['vin']
                        needs_update = True
                        print(f"VIN update needed for {update['serial']}: '{matching_device.get('vehicleIdentificationNumber', '')}' -> '{update['vin']}'")
                    
                    # Check group updates
                    current_group_ids = [g['id'] for g in updated_device.get('groups', [])]
                    update_group_ids = update['groups']
                    
                    # Build new groups list
                    new_groups = []
                    new_group_ids = []
                    
                    # Keep existing groups that are not changeable
                    for group in updated_device.get('groups', []):
                        if isinstance(group, dict) and group['id'] not in changeable_groups:
                            new_groups.append({'id': group['id']})
                            new_group_ids.append(group['id'])
                    
                    # Add new changeable groups from update
                    group_changes = []
                    for group_id in update_group_ids:
                        if group_id in changeable_groups and group_id not in new_group_ids:
                            new_groups.append({'id': group_id})
                            new_group_ids.append(group_id)
                            if group_id not in current_group_ids:
                                group_changes.append(group_id)
                                needs_update = True
                    
                    if group_changes:
                        print(f"Group updates needed for {update['serial']}: adding {group_changes}")
                    
                    updated_device['groups'] = new_groups
                    
                    if needs_update:
                        api_calls.append(('Set', {'typeName': 'Device', 'entity': updated_device}))
                        devices_to_update.append(matching_device)
                        print(f"Device {update['serial']} queued for update")
                    else:
                        print(f"No updates needed for device {update['serial']}")
                        
                except Exception as e:
                    print(f"Error processing update for {update.get('serial', 'unknown')}: {e}")
                    continue
            
            # Execute API calls
            if api_calls:
                print(f"Executing {len(api_calls)} device updates...")
                try:
                    results = self.api.multi_call(api_calls)
                    if isinstance(results, list):
                        print(f"Successfully updated {len(results)} devices")
                        
                        # Log successful updates
                        for i, result in enumerate(results):
                            device = devices_to_update[i]
                            print(f"Updated device: {device['serialNumber']} - {device['name']}")
                    else:
                        logging.warning("Unexpected response format from Geotab API")
                        
                except Exception as e:
                    print(f"Error executing API calls: {e}")
            else:
                print("No API calls needed - all devices are up to date")
                
        except Exception as e:
            print(f"Error in sync process: {e}")

    def process_chevin_sync(self):
        """Process the Chevin SFTP sync after reports are generated"""
        print("=== Starting Chevin-Geotab sync process ===")
        
        try:
            # Download CSV file from SFTP
            if not self.download_sftp_file():
                print("Failed to download CSV file from SFTP")
                return
            
            # Parse CSV updates
            updates = self.parse_csv_updates()
            if not updates:
                print("No valid updates found in CSV file")
                return
            
            # Sync with Geotab
            self.sync_vehicles_with_geotab(updates)
            
            print("=== Chevin sync completed ===")
            
        except Exception as e:
            print(f"Error in Chevin sync process: {e}")

    def generate_all_reports(self):
        """Generate all four reports and then process Chevin sync."""
        try:
            print("Starting report generation...")
            
            # Authenticate and cache reference data
            if not self.authenticate_geotab():
                print("Failed to authenticate. Skipping report generation.")
                return
            
            self.cache_reference_data()
            
            # Cache batch data for last hour
            self.cache_batch_data()
            
            # Generate all reports
            reports = {
                "Asset_Status_Report": self.generate_asset_status_report(),
                "Trips_History_Report": self.generate_trips_history_report(),
                "Exceptions_Details_Report": self.generate_exceptions_report(),
                "Engine_Faults_Report": self.generate_engine_faults_report()
            }
            
            # Save to Excel files
            #self.save_reports_to_excel(reports)

            self.save_reports_to_sftp(reports)
            
            print("All reports generated successfully!")
            
            # Now process Chevin sync
            self.process_chevin_sync()
            
        except Exception as e:
            print(f"Error generating reports: {e}")

def run_scheduled_reports():
    """Function to run the scheduled report generation."""
    generator = GeotabReportGenerator()
    generator.generate_all_reports()

def main():
    """Main function to set up and run the scheduler."""
    print("Starting Geotab Report Generator...")
    
    # Schedule the job to run every hour
    schedule.every().hour.do(run_scheduled_reports)
    
    # Run once immediately for testing
    print("Running initial report generation...")
    run_scheduled_reports()
    
    # Keep the scheduler running
    print("Scheduler started. Reports will be generated every hour.")
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    main()