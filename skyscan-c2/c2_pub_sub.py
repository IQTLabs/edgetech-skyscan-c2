"""This file includes a command and control module to direct and coordinate
different services using MQTT.
"""
import os
import json
import logging
from io import StringIO
from time import sleep, time
from typing import Any
import paho.mqtt.client as mqtt
from typing import Any, Dict, Union
import pandas as pd
import schedule
from datetime import datetime
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
        c2_topic: str,
        ledger_topic: str,
        object_topic: str,
        prioritized_ledger_topic: str,
        manual_override_topic: str,
        min_tilt: float,
        min_altitude: float,
        max_altitude: float,
        object_distance_threshold: str,
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
        self.c2_topic = c2_topic
        self.ledger_topic = ledger_topic
        self.object_topic = object_topic
        self.prioritized_ledger_topic = prioritized_ledger_topic
        self.manual_override_topic = manual_override_topic
        self.device_latitude = float(device_latitude)
        self.device_longitude = float(device_longitude)
        self.device_altitude = float(device_altitude)
        self.object_distance_threshold = float(object_distance_threshold)
        self.min_tilt = min_tilt
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
        logging.info(f"Initial E_XYZ_to_uvw: {self.E_XYZ_to_uvw}")

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
    c2_topic = {c2_topic}
    ledger_topic = {ledger_topic}
    object_topic = {object_topic}
    prioritized_ledger_topic = {prioritized_ledger_topic}
    manual_override_topic = {manual_override_topic}
    min_tilt = {min_tilt}
    min_altitude = {min_altitude}
    max_altitude = {max_altitude}
    object_distance_threshold = {object_distance_threshold}
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

        object_msg_age = datetime.utcnow().timestamp() - self.timestamp_o  # [s]
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
        logging.info(f"Camera pan and tilt to object: {self.rho_o}, {self.tau_o} [deg]")

        return self.rho_o, self.tau_o, self.distance3d

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
        except (KeyError, TypeError) as e:
            logging.error(f"Error: {e}")
            logging.error(json_payload)
            logging.error(
                f"Data payload type: {data_payload_type} not found in payload: {data_payload}"
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
        self.min_altitude = config.get("min_altitude", self.min_altitude)
        self.max_altitude = config.get("max_altitude", self.max_altitude)

    def _target_selection_callback(
        self: Any, _client: mqtt.Client, _userdata: Dict[Any, Any], msg: Any
    ) -> None:
        logging.debug("Ledger recieved")
        payload_dict = json.loads(str(msg.payload.decode("utf-8")))

        if "ObjectLedger" in payload_dict.keys():
            object_ledger_json = payload_dict["ObjectLedger"]
            object_ledger_df = pd.read_json(
                StringIO(object_ledger_json), convert_dates=False, convert_axes=False
            )
            object_ledger_df["age"] = time() - object_ledger_df["timestamp"]

            if len(object_ledger_df):
                logging.debug("Ledger not empty")
                ### some logic to select which target
                target = None

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

                object_ledger_df["min_tilt_fail"] = (
                    object_ledger_df["camera_tilt"] < self.min_tilt
                )
                object_ledger_df["min_altitude_fail"] = (
                    object_ledger_df["altitude"] < self.min_altitude
                )
                object_ledger_df["max_altitude_fail"] = (
                    object_ledger_df["altitude"] > self.max_altitude
                )

                logging.info(f"Object ledger: {object_ledger_df.to_string()}")
                if not object_ledger_df.empty and not self.override_object:
                    logging.debug("Standard distance thresholding")
                    object_ledger_df = object_ledger_df[
                        (
                            object_ledger_df["relative_distance"]
                            <= self.object_distance_threshold
                        )
                        & (object_ledger_df["min_tilt_fail"] == False)
                        & (object_ledger_df["min_altitude_fail"] == False)
                        & (object_ledger_df["max_altitude_fail"] == False)
                    ]
                    if not object_ledger_df.empty:
                        logging.debug("Object[s] within distance threshold")
                        target = object_ledger_df.sort_values(
                            by="relative_distance", ascending=True
                        ).iloc[0]
                    else:
                        logging.info("No object[s] within distance threshold")
                elif self.override_object and not object_ledger_df.empty:
                    logging.debug("Override object selection")
                    selection_df = object_ledger_df[
                        object_ledger_df.index == self.override_object
                    ]
                    selection_df = selection_df.sort_values(by="age", ascending=True)
                    if not selection_df.empty:
                        logging.debug("Override object exists")
                        target = selection_df.iloc[0]
                    else:
                        logging.debug("Override object not found")
                        target = None
                        self.override_object = None

                if target is not None:
                    logging.debug(
                        f"Payload ready, is override: {self.override_object is not None} or is standard: {self.override_object is None}"
                    )
                    payload = {
                        "timestamp": float(target["timestamp"]),
                        "data": {
                            "object_id": str(target.name),
                            "object_type": str(target["object_type"]),
                            "timestamp": float(target["timestamp"]),
                            "latitude": float(target["latitude"]),
                            "longitude": float(target["longitude"]),
                            "altitude": float(target["altitude"]),
                            "track": float(target["track"]),
                            "horizontal_velocity": float(target["horizontal_velocity"]),
                            "vertical_velocity": float(target["vertical_velocity"]),
                            "relative_distance": float(target["relative_distance"]),
                            "camera_tilt": float(target["camera_tilt"]),
                            "camera_pan": float(target["camera_pan"]),
                            "distance_3d": float(target["distance_3d"]),
                            "age": float(target["age"]),
                        },
                    }
                    logging.debug(f"This is the selected target {payload}")

                    out_json = self.generate_payload_json(
                        push_timestamp=str(int(datetime.utcnow().timestamp())),
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

                    out_json = self.generate_payload_json(
                        push_timestamp=str(int(datetime.utcnow().timestamp())),
                        device_type="Collector",
                        id_=self.hostname,
                        deployment_id=f"ShipScan-{self.hostname}",
                        current_location="-90, -180",
                        status="Debug",
                        message_type="Event",
                        model_version="null",
                        firmware_version="v0.0.0",
                        data_payload_type="Prioritized Object Ledger",
                        data_payload=json.dumps(object_ledger_df.to_json()),
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
        if "ObjectIDOverride" in payload_dict.keys():
            self.override_object = str(payload_dict["ObjectIDOverride"])
            ### end here

    def main(self: Any) -> None:
        """Main loop and function that setup the heartbeat to keep the TCP/IP
        connection alive and publishes the data to the MQTT broker every 10 minutes
        and keeps the main thread alive.
        """
        # publish heartbeat to keep the TCP/IP connection alive
        schedule.every(10).seconds.do(self.publish_heartbeat, payload="C2 Heartbeat")

        # every file interval, publish a message to broadcast to file
        # saving nodes to change files
        schedule.every(self.file_interval).minutes.do(
            self.publish_to_topic,
            topic_name=self.c2_topic,
            publish_payload=json.dumps({"msg": "NEW FILE"}),
        )
        self.add_subscribe_topic(self.config_topic, self._config_callback)
        self.add_subscribe_topic(self.ledger_topic, self._target_selection_callback)
        self.add_subscribe_topic(self.manual_override_topic, self._target_selection_callback)


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
        c2_topic=str(os.environ.get("C2_TOPIC")),
        ledger_topic=str(os.environ.get("LEDGER_TOPIC")),
        object_topic=str(os.environ.get("OBJECT_TOPIC")),
        prioritized_ledger_topic=str(os.environ.get("PRIORITIZED_LEDGER_TOPIC")),
        manual_override_topic=str(os.environ.get("MANUAL_OVERRIDE_TOPIC")),
        device_latitude=str(os.environ.get("TRIPOD_LATITUDE")),
        device_longitude=str(os.environ.get("TRIPOD_LONGITUDE")),
        device_altitude=str(os.environ.get("TRIPOD_ALTITUDE")),
        lead_time=float(os.environ.get("LEAD_TIME", 1.0)),
        min_tilt=float(os.environ.get("MIN_TILT", 0.0)),
        min_altitude=float(os.environ.get("MIN_ALTITUDE", 0.0)),
        max_altitude=float(os.environ.get("MAX_ALTITUDE", 100000000.0)),
        object_distance_threshold=str(os.environ.get("OBJECT_DISTANCE_THRESHOLD")),
        log_level=str(os.environ.get("LOG_LEVEL", "INFO")),
    )
    c2.main()
