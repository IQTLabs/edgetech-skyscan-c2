"""This file includes a command and control module to direct and coordinate
different services using MQTT.
"""
import os
import json
from json import JSONDecodeError
import logging
from io import StringIO
from time import sleep, time
from typing import Any
import paho.mqtt.client as mqtt
from typing import Any, Dict, Union
import pandas as pd
import schedule
from datetime import datetime, timezone
from math import radians, cos, sin, asin, sqrt

import math
import numpy as np

import axis_ptz_utilities
from base_mqtt_pub_sub import BaseMQTTPubSub


class C2PubSub(BaseMQTTPubSub):
    """The C2PubSub is a class that wraps command and control functionalities. Currently,
    this is limited to broadcasting to nodes to write to a new file.

    Args:
        BaseMQTTPubSub (BaseMQTTPubSub): parent class written in the EdgeTech Core module
    """

    FILE_INTERVAL = 1  # minutes
    EARTH_RADIUS_KM = 6371

    def __init__(
        self: Any,
        hostname: str,
        config_topic: str,
        ledger_topic: str,
        object_topic: str,
        prioritized_ledger_topic: str,
        manual_override_topic: str,
        faa_master_csv: str, 
        faa_acftref_csv: str, 
        min_tilt: float,
        max_tilt: float,
        min_altitude: float,
        max_altitude: float,
        mapping_filepath: str,
        object_distance_threshold: str,
        distance_improvement_threshold: float,
        device_latitude: str,
        device_longitude: str,
        device_altitude: str,
        lead_time: float,
        file_interval: int = FILE_INTERVAL,
        earth_radius_km: int = EARTH_RADIUS_KM,
        debug: bool = False,
        log_level: str = "INFO",
        **kwargs: Any,
    ) -> None:
        """The constructor of the C2PubSub class takes a topic name to broadcast
        to and an interval to broadcast to that topic at.

        # TODO: Update?
        Args:
            next_file_topic (str): The MQTT topic to broadcast a payload that changes
            the file at a given interval.
            file_interval (int): The number of minutes before C2 broadcasts a message
            to write to a new file.Defaults to FILE_INTERVAL.
            log_level (str): One of 'NOTSET', 'DEBUG', 'INFO', 'WARN',
            'WARNING', 'ERROR', 'FATAL', 'CRITICAL'
        """
        super().__init__(**kwargs)
        # initialize attributes
        self.hostname = hostname
        self.config_topic = config_topic
        self.ledger_topic = ledger_topic
        self.object_topic = object_topic
        self.prioritized_ledger_topic = prioritized_ledger_topic
        self.manual_override_topic = manual_override_topic
        self.faa_master_csv = faa_master_csv
        self.faa_acftref_csv = faa_acftref_csv
        self.device_latitude = float(device_latitude)
        self.device_longitude = float(device_longitude)
        self.device_altitude = float(device_altitude)
        self.mapping_filepath = mapping_filepath
        self.object_distance_threshold = float(object_distance_threshold)
        self.distance_improvement_threshold = float(distance_improvement_threshold)
        self.min_tilt = min_tilt
        self.max_tilt = max_tilt
        self.min_altitude = min_altitude
        self.max_altitude = max_altitude
        self.lambda_t = self.device_longitude  # [deg]
        self.varphi_t = self.device_latitude  # [deg]
        self.h_t = self.device_altitude  # [m]
        self.lead_time = lead_time  # [s]
        self.alpha = 0.0  # [deg]
        self.beta = 0.0
        self.gamma = 0.0
        self.rho_c = 0.0  # [deg]
        self.tau_c = 0.0  # [deg]
        self.file_interval = file_interval
        self.earth_radius_km = earth_radius_km
        self.debug = debug
        self.log_level = log_level
        self.override_object = None
        self.tracked_object = None

        if self.mapping_filepath == "" or not os.path.isfile(self.mapping_filepath):
            self.occlusion_mapping_enabled = False
        else:
            with open(mapping_filepath) as f:
                self.occlusion_mapping = json.load(f)
            self.occlusion_mapping_enabled = True

        self.faa_master_df = pd.read_csv(self.faa_master_csv, converters={"MODE S CODE HEX": str.strip}, index_col="MODE S CODE HEX")
        self.faa_acftref_df = pd.read_csv(self.faa_acftref_csv, converters={"CODE": str.strip},index_col="CODE")

        # Compute tripod position in the geocentric (XYZ) coordinate
        # system
        self.r_XYZ_t = axis_ptz_utilities.compute_r_XYZ(
            self.lambda_t, self.varphi_t, self.h_t
        )

        # Compute orthogonal transformation matrix from geocentric
        # (XYZ) to topocentric (ENz) coordinates
        (
            self.E_XYZ_to_ENz,
            self.e_E_XYZ,
            self.e_N_XYZ,
            self.e_z_XYZ,
        ) = axis_ptz_utilities.compute_E_XYZ_to_ENz(self.lambda_t, self.varphi_t)

        # Compute the rotations from the geocentric (XYZ) coordinate
        # system to the camera housing fixed (uvw) coordinate system

        (
            self.q_alpha,
            self.q_beta,
            self.q_gamma,
            self.E_XYZ_to_uvw,
            _,
            _,
            _,
        ) = axis_ptz_utilities.compute_camera_rotations(
            self.e_E_XYZ,
            self.e_N_XYZ,
            self.e_z_XYZ,
            self.alpha,
            self.beta,
            self.gamma,
            self.rho_c,
            self.tau_c,
        )
        

        # create MQTT client connection
        self.connect_client()
        sleep(1)
        self.publish_registration("C2 Registration")
        logging.info("Connected to MQTT Broker")

        # Log configuration parameters
        logging.info(
            f"""C2PubSub initialized with parameters:
    hostname = {hostname}
    config_topic = {config_topic}
    ledger_topic = {ledger_topic}
    object_topic = {object_topic}
    prioritized_ledger_topic = {prioritized_ledger_topic}
    manual_override_topic = {manual_override_topic}
    min_tilt = {min_tilt}
    max_tilt = {max_tilt}
    min_altitude = {min_altitude}
    max_altitude = {max_altitude}
    mapping_filepath = {mapping_filepath}
    object_distance_threshold = {object_distance_threshold}
    distance_improvement_threshold = {distance_improvement_threshold}
    device_latitude = {device_latitude}
    device_longitude = {device_longitude}
    device_altitude = {device_altitude}
    file_interval = {file_interval}
    earth_radius_km = {earth_radius_km}
    debug = {debug}
    log_level = {log_level}
            """
        )

    def _calculate_camera_angles(self: Any, data: Any) -> tuple[float, float, float]:
        # Calculate the relative tilt and pan angles of the object compared to the device
        # Your calculation logic here

        # Assign identifier, time, position, and velocity of the
        # object

        if not set(
            [
                "timestamp",
                "latitude",
                "longitude",
                "altitude",
                "track",
                "horizontal_velocity",
                "vertical_velocity",
            ]
        ) <= set(data.keys()):
            logging.info(f"Required keys missing from object message data: {data}")
            return 0.0, 0.0, 0.0
        # logging.info(f"Processing object msg data: {data}")
        self.timestamp_o = float(data["timestamp"])  # [s]
        self.timestamp_c = self.timestamp_o
        self.lambda_o = data["longitude"]  # [deg]
        self.varphi_o = data["latitude"]  # [deg]
        self.h_o = data["altitude"]  # [m]
        track_o = data["track"]  # [deg]
        ground_speed_o = data["horizontal_velocity"]  # [m/s]
        vertical_rate_o = data["vertical_velocity"]  # [m/s]

        try:
            # Compute position in the geocentric (XYZ) coordinate system
            # of the object relative to the tripod at time zero, the
            # observation time
            r_XYZ_o_0 = axis_ptz_utilities.compute_r_XYZ(
                self.lambda_o, self.varphi_o, self.h_o
            )
            r_XYZ_o_0_t = r_XYZ_o_0 - self.r_XYZ_t

            # Assign lead time, computing and adding age of object
            # message, if enabled
            lead_time = self.lead_time  # [s]

            object_msg_age = time() - self.timestamp_o  # [s]
            logging.debug(
                f"Object msg age: {object_msg_age} [s] Lead time: {lead_time} [s]"
            )
            lead_time += object_msg_age

            # Compute position and velocity in the topocentric (ENz)
            # coordinate system of the object relative to the tripod at
            # time zero, and position at slightly later time one
            self.r_ENz_o_0_t = np.matmul(self.E_XYZ_to_ENz, r_XYZ_o_0_t)
            track_o = math.radians(track_o)
            self.v_ENz_o_0_t = np.array(
                [
                    ground_speed_o * math.sin(track_o),
                    ground_speed_o * math.cos(track_o),
                    vertical_rate_o,
                ]
            )
            r_ENz_o_1_t = self.r_ENz_o_0_t + self.v_ENz_o_0_t * lead_time

            # Compute position, at time one, and velocity, at time zero,
            # in the geocentric (XYZ) coordinate system of the object
            # relative to the tripod
            r_XYZ_o_1_t = np.matmul(self.E_XYZ_to_ENz.transpose(), r_ENz_o_1_t)
            v_XYZ_o_0_t = np.matmul(self.E_XYZ_to_ENz.transpose(), self.v_ENz_o_0_t)

            # Compute the distance between the object and the tripod at
            # time one
            self.distance3d = axis_ptz_utilities.norm(r_ENz_o_1_t)

            # TODO: Restore?
            # Compute the distance between the object and the tripod
            # along the surface of a spherical Earth
            # distance2d = axis_ptz_utilities.compute_great_circle_distance(
            #     self.self.lambda_t,
            #     self.varphi_t,
            #     self.lambda_o,
            #     self.varphi_o,
            # )  # [m]

            # Compute the object azimuth and elevation relative to the
            # tripod
            self.azm_o = math.degrees(math.atan2(r_ENz_o_1_t[0], r_ENz_o_1_t[1]))  # [deg]
            self.elv_o = math.degrees(
                math.atan2(r_ENz_o_1_t[2], axis_ptz_utilities.norm(r_ENz_o_1_t[0:2]))
            )  # [deg]
            # logging.info(f"Object azimuth and elevation: {self.azm_o}, {self.elv_o} [deg]")

            # Compute pan and tilt to point the camera at the object
            r_uvw_o_1_t = np.matmul(self.E_XYZ_to_uvw, r_XYZ_o_1_t)
            self.rho_o = math.degrees(math.atan2(r_uvw_o_1_t[0], r_uvw_o_1_t[1]))  # [deg]
            self.tau_o = math.degrees(
                math.atan2(r_uvw_o_1_t[2], axis_ptz_utilities.norm(r_uvw_o_1_t[0:2]))
            )  # [deg]

            return self.rho_o, self.tau_o, self.distance3d
        except Exception as e:
            logging.error(f"Error: {e} latitude: {self.varphi_o}, longitude: {self.lambda_o}, altitude: {self.h_o}")
            return 0.0, 0.0, 0.0

    def _relative_distance_meters(
        self: Any, lat_one: float, lon_one: float, lat_two: float, lon_two: float
    ) -> float:
        """gives an Earth-as-a-sphere-based distance approximation using the Haversine formula

        Args:
            lat_one (float): latitude of coordindate one
            lon_one (float): longitude of coordindate one
            lat_two (float): latitude of coordindate two
            lon_two (float): longitude of coordindate two

        Returns:
            str: integer distance in metters with the unit abbreviation
        """
        lat_one, lon_one, lat_two, lon_two = (
            radians(lat_one),
            radians(lon_one),
            radians(lat_two),
            radians(lon_two),
        )

        # Haversine formula
        return float(
            (
                2
                * asin(
                    sqrt(
                        sin((lat_two - lat_one) / 2) ** 2
                        + cos(lat_one)
                        * cos(lat_two)
                        * sin((lon_two - lon_one) / 2) ** 2
                    )
                )
                * self.earth_radius_km
            )
            * 1000
        )

    def decode_payload(
        self, msg: Union[mqtt.MQTTMessage, str], data_payload_type: str
    ) -> Dict[Any, Any]:
        """
        Decode the payload carried by a message.

        Parameters
        ----------
        payload: mqtt.MQTTMessage
            The MQTT message
        data_payload_type: str
            The data payload type

        Returns
        -------
        data : Dict[Any, Any]
            The data payload of the message payload
        """
        if type(msg) == mqtt.MQTTMessage:
            payload = msg.payload.decode()
        else:
            payload = msg
        try:
            json_payload = json.loads(payload)
            data_payload = json_payload[data_payload_type]
        except (KeyError, TypeError, JSONDecodeError, json.JSONDecodeError) as e:
            logging.error(f"Error: {e}")
            logging.error(payload)
            logging.error(
                f"Error decoding payload: {payload}"
            )
            return {}
        return json.loads(data_payload)

    def _config_callback(
        self,
        _client: Union[mqtt.Client, None],
        _userdata: Union[Dict[Any, Any], None],
        msg: Union[mqtt.MQTTMessage, str],
    ) -> None:
        """
        Process configuration message.

        Parameters
        ----------
        _client: Union[mqtt.Client, None]
            MQTT client
        _userdata: Union[Dict[Any, Any], None]
            Any required user data
        msg: Union[mqtt.MQTTMessage, Dict[Any, Any]]
            An MQTT message, or dictionary

        Returns
        -------
        None
        """
        # Assign data attributes allowed to change during operation,
        # ignoring config message data without a "axis-ptz-controller"
        # key

        data = self.decode_payload(msg, "Configuration")
        if "skyscan-c2" not in data:
            logging.info(f"Configuration message data missing skyscan-c2: {data}")
            return
        logging.info(f"Processing config msg data: {data}")
        config = data["skyscan-c2"]
        self.min_tilt = config.get("min_tilt", self.min_tilt)
        self.max_tilt = config.get("max_tilt", self.max_tilt)
        self.min_altitude = config.get("min_altitude", self.min_altitude)
        self.max_altitude = config.get("max_altitude", self.max_altitude)

    def _add_faa_info(self, object_ledger_df: pd.DataFrame) -> pd.DataFrame:
        """Add FAA information to the object ledger

        Args:
            object_ledger_df (pd.DataFrame): DataFrame of objects

        Returns:
            pd.DataFrame: DataFrame of objects with FAA information
        """

        # logging.info("Adding FAA info")
        # logging.info(object_ledger_df)
        # logging.info("columns:", self.faa_master_df.columns.tolist())
        # logging.info(self.faa_master_df.iloc[0])
        try:
            reg_matching_rows = self.faa_master_df.loc[object_ledger_df.name.upper()]
        except KeyError as e:
            logging.debug(f"Aircraft not found in FAA data: {e} ")
            return object_ledger_df
        except IndexError as e:
            logging.info("indexerror")
            logging.info(e)
            return object_ledger_df
        except Exception as e:
            logging.info(f"random error {e}")
        #logging.info(object_ledger_df.name.upper())
        if not reg_matching_rows.empty:
            object_ledger_df['n_number'] = reg_matching_rows['N-NUMBER'].strip()
            object_ledger_df['owner'] = reg_matching_rows['NAME'].strip()
            object_ledger_df['aircraft_type'] = reg_matching_rows['TYPE AIRCRAFT']
            object_ledger_df['engine_type'] = reg_matching_rows['TYPE ENGINE']


            code = reg_matching_rows['MFR MDL CODE'].strip()
            # get MFR and MODEL from ACFTREF.csv
            acft_matching_rows = self.faa_acftref_df.loc[code]
  
            if not acft_matching_rows.empty:
                object_ledger_df['aircraft_mfr']  = acft_matching_rows['MFR'].strip()
                object_ledger_df['aircraft_model'] = acft_matching_rows['MODEL'].strip()
                object_ledger_df['num_engine'] = acft_matching_rows['NO-ENG']

        #logging.info(object_ledger_df)
        return object_ledger_df

    def _elevation_check(self: Any, azimuth: float, elevation: float) -> bool:
        """Check if the elevation is within the acceptable range

        Args:
            elevation (float): The elevation to check

        Returns:
            bool: True if the elevation is within the acceptable range
        """

        # Check if Occlusion Mapping is enabled
        # Occlusion is designed for structures that come up from the horizon and block the view of the camera.
        # It doesn't work for overhanging things.
        if self.occlusion_mapping_enabled:

            # Go through all of the Occlusion Mapping Points
            for i, obj in enumerate(self.occlusion_mapping):
                # Check if the Azimuth(Pan) is greater or if this is the last point. If it is the last point,
                # then we can assume it applies to the end of the pan (360)
                if (obj["azimuth"] > azimuth) or (i == len(self.occlusion_mapping) - 1):

                    # If the Occlusion Point elevation is greater than the current elevation, then it is occluded.
                    if  self.max_tilt > elevation > obj["elevation"]:
                        return True
                    else:
                        return False
                    break
            if self.min_tilt > elevation:
                return True
            else:
                return False
        else:
            return self.min_tilt <= elevation <= self.max_tilt
        
    def _target_selection_callback(
        self: Any, _client: mqtt.Client, _userdata: Dict[Any, Any], msg: Any
    ) -> None:
        payload_dict = json.loads(str(msg.payload.decode("utf-8")))

        try:
            if "ObjectLedger" in payload_dict.keys():
                object_ledger_json = payload_dict["ObjectLedger"]
                object_ledger_df = pd.read_json(
                    StringIO(object_ledger_json), convert_dates=False, convert_axes=False
                )


                if len(object_ledger_df):
                    object_ledger_df = object_ledger_df.apply(lambda x: self._add_faa_info(x), axis=1)
                    # Check and log any rows missing a timestamp
                    missing_timestamp_rows = object_ledger_df[~object_ledger_df['timestamp'].notna()]
                    if not missing_timestamp_rows.empty:
                        logging.warning(f"Rows missing timestamp: {missing_timestamp_rows}")
                    ### some logic to select which target
                    object_ledger_df["age"] = time() - object_ledger_df["timestamp"]
                    object_ledger_df["target"] = False
                    object_ledger_df["selectable"] = False

                    (
                        object_ledger_df["camera_pan"],
                        object_ledger_df["camera_tilt"],
                        object_ledger_df["distance_3d"],
                    ) = zip(
                        *object_ledger_df.apply(
                            lambda x: self._calculate_camera_angles(x.to_dict()),
                            axis=1,
                        )
                    )
                    object_ledger_df["relative_distance"] = object_ledger_df.apply(
                        lambda x: self._relative_distance_meters(
                            self.device_latitude,
                            self.device_longitude,
                            float(x["latitude"]),
                            float(x["longitude"]),
                        ),
                        axis=1,
                    )

                    object_ledger_df["tilt_fail"] = object_ledger_df.apply(
                        lambda x: not self._elevation_check(x["camera_pan"], x["camera_tilt"]),
                        axis=1
                    )


                    object_ledger_df["min_altitude_fail"] = (
                        object_ledger_df["altitude"] < self.min_altitude
                    )
                    object_ledger_df["max_altitude_fail"] = (
                        object_ledger_df["altitude"] > self.max_altitude
                    )

                    # check if we have any objects in the ledger 
                    if not object_ledger_df.empty: 
                        
                        # do we have an override object set?
                        if self.override_object:
                            logging.debug("Override object selection")
                            selection_df = object_ledger_df.loc[
                                object_ledger_df.index == self.override_object
                            ]
                            selection_df = selection_df.sort_values(by="age", ascending=True)
                            if not selection_df.empty:
                                logging.debug(f"Selecting Override object: {self.override_object}")
                                self.tracked_object = selection_df.iloc[0]
                            else:
                                logging.info(f"Override object {self.override_object} not found in Ledger, clearing override object")
                                self.tracked_object = None
                                self.override_object = None
                        else:
                            # select a subset of the ledge that meets the criteria
                            target_ledger_df = object_ledger_df[
                                (
                                    object_ledger_df["relative_distance"]
                                    <= self.object_distance_threshold
                                )
                                & (object_ledger_df["tilt_fail"] == False)
                                & (object_ledger_df["min_altitude_fail"] == False)
                                & (object_ledger_df["max_altitude_fail"] == False)
                            ]

                            # are there any objects that meet the criteria?
                            if not target_ledger_df.empty:
                                logging.debug("Object[s] within distance threshold")
                                target_ledger_df.loc[:, "selectable"] = True
                                potential_target = target_ledger_df.sort_values(
                                    by="relative_distance", ascending=True
                                ).iloc[0]
                                potential_target.loc["target"] = True


                                # are we currently tracking an object?
                                if self.tracked_object is not None:
    
                                    current_target_ledger = target_ledger_df.loc[target_ledger_df.index == self.tracked_object.name]
                                    
                                    # is the current target still within the criteria?
                                    if not current_target_ledger.empty:
                                        current_target = current_target_ledger.iloc[0]

                                        # is there a potential target that is closer and over the threshold?
                                        if potential_target is not None:
                                            distance_improvement_percent = (current_target["relative_distance"] - potential_target["relative_distance"]) / current_target["relative_distance"]
                                            if distance_improvement_percent > self.distance_improvement_threshold:
                                                logging.info(f"Switching Aircraft - Improvement in distance: {distance_improvement_percent} (percent)")
                                                self.tracked_object = potential_target
                                            else:
                                                self.tracked_object = current_target
                                    
                                    # handle the case where the current target is no longer within the criteria
                                    else:
                                        logging.info("Switching Aircraft - Current target no longer within criteria")
                                        self.tracked_object = potential_target

                                # handle the case where we are not currently tracking an object, and there is a potential target
                                elif potential_target is not None:
                                    self.tracked_object = potential_target
                                else:
                                    self.tracked_object = None


                            else:
                                logging.debug("No object[s] within distance threshold")
                                self.tracked_object = None



                    if self.tracked_object is not None:
                        logging.debug(
                            f"Payload ready, is override: {self.override_object is not None} or is standard: {self.override_object is None}"
                        )
                        payload = {
                            "timestamp": float(self.tracked_object["timestamp"]),
                            "data": {
                                "object_id": str(self.tracked_object.name),
                                "object_type": str(self.tracked_object["object_type"]),
                                "timestamp": float(self.tracked_object["timestamp"]),
                                "latitude": float(self.tracked_object["latitude"]),
                                "longitude": float(self.tracked_object["longitude"]),
                                "altitude": float(self.tracked_object["altitude"]),
                                "track": float(self.tracked_object["track"]),
                                "horizontal_velocity": float(self.tracked_object["horizontal_velocity"]),
                                "vertical_velocity": float(self.tracked_object["vertical_velocity"]),
                                "relative_distance": float(self.tracked_object["relative_distance"]),
                                "camera_tilt": float(self.tracked_object["camera_tilt"]),
                                "camera_pan": float(self.tracked_object["camera_pan"]),
                                "distance_3d": float(self.tracked_object["distance_3d"]),
                                "flight": str(self.tracked_object["flight"]),
                                "squawk": str(self.tracked_object["squawk"]),
                                "category": str(self.tracked_object["category"]),
                                "emergency": str(self.tracked_object["emergency"]),
                                "aircraft_type": str(self.tracked_object.get("aircraft_type", "")),
                                "aircraft_mfr": str(self.tracked_object.get("aircraft_mfr", "")),
                                "aircraft_model": str(self.tracked_object.get("aircraft_model", "")),
                                "n_number": str(self.tracked_object.get("n_number","")),
                                "owner": str(self.tracked_object.get("owner", "")),
                                "engine_type": str(self.tracked_object.get("engine_type","")),
                                "num_engine": str(self.tracked_object.get("num_engine",0)),
                                "age": float(self.tracked_object["age"]),
                            },
                        }
                        logging.debug(f"This is the selected target {payload}")

                        out_json = self.generate_payload_json(
                            push_timestamp=str(int(datetime.now(timezone.utc).timestamp())),
                            device_type="Collector",
                            id_=self.hostname,
                            deployment_id=f"ShipScan-{self.hostname}",
                            current_location="-90, -180",
                            status="Debug",
                            message_type="Event",
                            model_version="null",
                            firmware_version="v0.0.0",
                            data_payload_type="Selected Object",
                            data_payload=json.dumps(payload["data"]),
                        )

                        success = self.publish_to_topic(self.object_topic, out_json)
                        if success:
                            logging.debug(
                                f"Successfully sent data: {out_json} on topic: {self.object_topic}"
                            )
                        else:
                            logging.warning(
                                f"Failed to send data: {out_json} on topic: {self.object_topic}"
                            )
                    else:
                        out_json = self.generate_payload_json(
                            push_timestamp=str(int(datetime.now(timezone.utc).timestamp())),
                            device_type="Collector",
                            id_=self.hostname,
                            deployment_id=f"ShipScan-{self.hostname}",
                            current_location="-90, -180",
                            status="Debug",
                            message_type="Event",
                            model_version="null",
                            firmware_version="v0.0.0",
                            data_payload_type="Selected Object",
                            data_payload=json.dumps({}),
                        )

                        success = self.publish_to_topic(self.object_topic, out_json)
                        if success:
                            logging.debug(
                                f"Successfully sent data: {out_json} on topic: {self.object_topic}"
                            )
                        else:
                            logging.warning(
                                f"Failed to send data: {out_json} on topic: {self.object_topic}"
                            )


                    out_json = self.generate_payload_json(
                        push_timestamp=str(int(datetime.now(timezone.utc).timestamp())),
                        device_type="Collector",
                        id_=self.hostname,
                        deployment_id=f"ShipScan-{self.hostname}",
                        current_location="-90, -180",
                        status="Debug",
                        message_type="Event",
                        model_version="null",
                        firmware_version="v0.0.0",
                        data_payload_type="Prioritized Object Ledger",
                        data_payload=object_ledger_df.to_json(),
                    )

                    success = self.publish_to_topic(
                        self.prioritized_ledger_topic, out_json
                    )
                    if success:
                        logging.debug(
                            f"Successfully sent data: {out_json} on topic: {self.object_topic}"
                        )
                    else:
                        logging.warning(
                            f"Failed to send data: {out_json} on topic: {self.object_topic}"
                            )
        except (KeyError, TypeError) as e:
            logging.error(f"Error: {e}")
            logging.error(
                f"Error decoding parsing Ledger: {object_ledger_df}"
            )
        if "ObjectIDOverride" in payload_dict.keys():
            self.override_object = str(payload_dict["ObjectIDOverride"])
            logging.info(f"Override object set to: {self.override_object}")
            ### end here

    def main(self: Any) -> None:
        """Main loop and function that setup the heartbeat to keep the TCP/IP
        connection alive and publishes the data to the MQTT broker every 10 minutes
        and keeps the main thread alive.
        """
        # publish heartbeat to keep the TCP/IP connection alive
        schedule.every(10).seconds.do(self.publish_heartbeat, payload="C2 Heartbeat")


        self.add_subscribe_topic(self.config_topic, self._config_callback)
        self.add_subscribe_topic(self.ledger_topic, self._target_selection_callback)
        self.add_subscribe_topic(
            self.manual_override_topic, self._target_selection_callback
        )

        while True:
            try:
                # flush pending scheduled tasks
                schedule.run_pending()
                # sleep to avoid running at CPU time
                sleep(0.001)
            except KeyboardInterrupt as exception:
                if self.debug:
                    print(exception)


if __name__ == "__main__":
    c2 = C2PubSub(
        hostname=str(os.environ.get("HOSTNAME")),
        mqtt_ip=str(os.environ.get("MQTT_IP")),
        config_topic=os.environ.get("CONFIG_TOPIC", ""),
        ledger_topic=str(os.environ.get("LEDGER_TOPIC")),
        object_topic=str(os.environ.get("OBJECT_TOPIC")),
        prioritized_ledger_topic=str(os.environ.get("PRIORITIZED_LEDGER_TOPIC")),
        manual_override_topic=str(os.environ.get("MANUAL_OVERRIDE_TOPIC")),
        faa_master_csv=os.getenv("FAA_MASTER_CSV", "MASTER.txt.zst"),
        faa_acftref_csv=os.getenv("FAA_ACFTREF_CSV", "ACFTREF.txt.zst"),
        device_latitude=float(os.environ.get("TRIPOD_LATITUDE")),
        device_longitude=float(os.environ.get("TRIPOD_LONGITUDE")),
        device_altitude=float(os.environ.get("TRIPOD_ALTITUDE")),
        lead_time=float(os.environ.get("LEAD_TIME", 1.0)),
        min_tilt=float(os.environ.get("MIN_TILT", 0.0)),
        max_tilt=float(os.environ.get("MAX_TILT", 90.0)),
        min_altitude=float(os.environ.get("MIN_ALTITUDE", 0.0)),
        max_altitude=float(os.environ.get("MAX_ALTITUDE", 100000000.0)),
        mapping_filepath=str(os.environ.get("MAPPING_FILEPATH","")),
        object_distance_threshold=str(os.environ.get("OBJECT_DISTANCE_THRESHOLD")),
        distance_improvement_threshold=float(os.environ.get("DISTANCE_IMPROVEMENT_THRESHOLD", 0.1)),
        log_level=str(os.environ.get("LOG_LEVEL", "INFO")),
    )
    c2.main()
