import paho.mqtt.client as mqtt
import sqlite3
import json
import time
import os
import threading
import google.generativeai as genai
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
CONFIG = {
    "local_mqtt": {
        "broker": "192.168.89.140",
        "port": 1883,
        "data_topic": "sensors/data",
        "alarm_topic": "sensors/alarm",
        "notification_topic": "sensors/notification",
        "control_alarma_topic": "sensors/control_alarma"
    },
    "ubidots": {
        "token": "BBUS-KM9l4FTFaXqFiFpnvmELe07QxJfUO3",
        "device": "fruit_monitor",
        "broker": "industrial.api.ubidots.com",
        "topic": "/v1.6/devices/fruit_monitor",
        "alarm_topic": "/v1.6/devices/fruit_monitor/alarm"
    },
    "database": {
        "path": "/home/pi/fruit_monitor_project/sensor_data.db"
    },
    "send_interval": 30,
    "ethylene_threshold": 0.75
}

# Google Gemini API Configuration
try:
    genai.configure(api_key="AIzaSyALLE0pjVzWB6Ls3x1F6szExf3JUOaSntw")
    SYSTEM_CONTEXT = (
        "Eres un experto en análisis de datos de sensores para frutas y verduras. "
        "Tu tarea es analizar datos como niveles de gas etileno (en ppm), temperatura (en °C) y humedad (en %), "
        "y proporcionar un pronóstico de descomposición y consejos prácticos para bananos."
    )
    MODEL = genai.GenerativeModel("gemini-1.5-flash")
    CHAT = MODEL.start_chat(history=[
        {"role": "user", "parts": [SYSTEM_CONTEXT]},
        {"role": "model", "parts": ["Entendido, estoy listo para analizar los datos."]}
    ])
except Exception as e:
    logging.error(f"Failed to initialize Gemini API: {e}")
    raise

def init_db():
    """Initialize the SQLite database."""
    try:
        db_dir = os.path.dirname(CONFIG["database"]["path"])
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)
        
        conn = sqlite3.connect(CONFIG["database"]["path"])
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS sensor_data
                     (timestamp TEXT, temperature REAL, humidity REAL, ethylene REAL, alarm INTEGER)''')
        conn.commit()
        conn.close()
        logging.info("Database initialized.")
    except Exception as e:
        logging.error(f"Failed to initialize database: {e}")
        raise

def store_data(temperature, humidity, ethylene, alarm):
    """Store sensor data in SQLite database."""
    try:
        conn = sqlite3.connect(CONFIG["database"]["path"])
        c = conn.cursor()
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        c.execute("INSERT INTO sensor_data (timestamp, temperature, humidity, ethylene, alarm) VALUES (?, ?, ?, ?, ?)",
                  (timestamp, temperature, humidity, ethylene, alarm))
        conn.commit()
        conn.close()
        logging.info(f"Data stored: timestamp={timestamp}, temperature={temperature}, humidity={humidity}, ethylene={ethylene}, alarm={alarm}")
    except Exception as e:
        logging.error(f"Failed to store data: {e}")
        raise

def get_latest_data():
    """Retrieve the latest sensor data from the database."""
    try:
        conn = sqlite3.connect(CONFIG["database"]["path"])
        c = conn.cursor()
        c.execute("SELECT temperature, humidity, ethylene, alarm FROM sensor_data ORDER BY timestamp DESC LIMIT 1")
        result = c.fetchone()
        conn.close()
        return result
    except Exception as e:
        logging.error(f"Failed to retrieve latest data: {e}")
        return None

def send_to_ubidots(temperature, humidity, ethylene, alarm):
    """Send sensor data to Ubidots."""
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(CONFIG["ubidots"]["token"])
    try:
        client.connect(CONFIG["ubidots"]["broker"], 1883, 60)
        payload = {
            "temperature": temperature,
            "humidity": humidity,
            "ethylene": ethylene,
            "alarm": alarm
        }
        client.publish(CONFIG["ubidots"]["topic"], json.dumps(payload), qos=1)
        client.disconnect()
        logging.info(f"Data sent to Ubidots: {payload}")
    except Exception as e:
        logging.error(f"Error sending to Ubidots: {e}")

def send_gemini_message_to_ubidots(message):
    """Send the Gemini message (now a number) as a numeric variable to Ubidots."""
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(CONFIG["ubidots"]["token"])
    
    try:
        client.connect(CONFIG["ubidots"]["broker"], 1883, 60)
        payload = {"gemini_message2": float(message)}  # Convert message to float to ensure numeric handling
        client.publish(CONFIG["ubidots"]["topic"], json.dumps(payload), qos=1)
        client.disconnect()
        logging.info(f"Gemini message sent to Ubidots: {message}")
    except Exception as e:
        logging.error(f"Error sending Gemini message to Ubidots: {e}")

def process_gemini_alert(ethylene, temperature, humidity):
    """Process sensor data with Gemini API and return alert message."""
    try:
        if ethylene is None or temperature is None or humidity is None:
            logging.error("Incomplete sensor data.")
            return "Incomplete sensor data."
        sensor_data = f"Etileno: {ethylene} ppm, Temperatura: {temperature} °C, Humedad: {humidity} %"
        message = f"Datos de sensores: {sensor_data}\n Dame un pronostico de dias de cuando se pudre el banano. Recuerda solo dame el numero según los datos que te envíe."
        logging.info(f"Sending to Gemini API: {message}")
        response = CHAT.send_message(message)
        logging.debug(f"Gemini api Response: {response.text}")
        return response.text if response.text else "gemini response is empty"
    except Exception as e:
        logging.error(f"Error in Gemini API: {e}")
        return "Error in analysis."

def on_connect_local(client, userdata, flags, reason_code, properties):
    """Callback for local MQTT connection."""
    logging.info(f"Connected to local MQTT broker with code {reason_code}")
    client.subscribe(CONFIG["local_mqtt"]["data_topic"])
    client.subscribe(CONFIG["local_mqtt"]["control_alarma_topic"])

def on_message_local(client, userdata, msg):
    """Callback for local MQTT messages."""
    try:
        raw_message = msg.payload.decode()
        logging.info(f"Raw message received on {msg.topic}: {raw_message}")
        
        if msg.topic == CONFIG["local_mqtt"]["data_topic"]:
            data = json.loads(raw_message)
            
            required_keys = ["temperature", "humidity", "ethylene", "alarm"]
            if not all(key in data for key in required_keys):
                missing_keys = [key for key in required_keys if key not in data]
                raise KeyError(f"Missing keys in JSON: {missing_keys}")
            
            temperature = float(data["temperature"])
            humidity = float(data["humidity"])
            ethylene = float(data["ethylene"])
            alarm = int(data["alarm"])
            
            logging.info(f"Processed data: temperature={temperature}, humidity={humidity}, ethylene={ethylene}, alarm={alarm}")
            
            store_data(temperature, humidity, ethylene, alarm)
            send_to_ubidots(temperature, humidity, ethylene, alarm)
            
            if ethylene > CONFIG["ethylene_threshold"]:
                notification = process_gemini_alert(ethylene, temperature, humidity)
                send_gemini_message_to_ubidots(notification)
                logging.info(f"Alert: Ethylene threshold exceeded! Notification: {notification}")
                client.publish(CONFIG["local_mqtt"]["notification_topic"], notification, qos=1)
                logging.info(f"Notification published to {CONFIG['local_mqtt']['notification_topic']}")
                client.publish(CONFIG["local_mqtt"]["alarm_topic"], "ON", qos=1)
                logging.info(f"Alarm ON published to {CONFIG['local_mqtt']['alarm_topic']}")
                
        elif msg.topic == CONFIG["local_mqtt"]["control_alarma_topic"]:
            command = raw_message.upper()
            if command in ["ON", "OFF"]:
                logging.info(f"Received control_alarma command: {command}")
                client.publish(CONFIG["local_mqtt"]["alarm_topic"], command, qos=1)
                logging.info(f"Forwarded to {CONFIG['local_mqtt']['alarm_topic']}: {command}")
            else:
                logging.warning(f"Invalid control_alarma command: {raw_message}")
                
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON: {e} - Raw message: {raw_message}")
    except KeyError as e:
        logging.error(f"Key error: {e}")
    except Exception as e:
        logging.error(f"Error processing local message: {e}")

def on_connect_ubidots(client, userdata, flags, reason_code, properties):
    """Callback for Ubidots MQTT connection."""
    logging.info(f"Connected to Ubidots MQTT broker with code {reason_code}")
    client.subscribe(CONFIG["ubidots"]["alarm_topic"])

def on_message_ubidots(client, userdata, msg):
    """Callback for Ubidots MQTT messages."""
    try:
        raw_message = msg.payload.decode()
        logging.info(f"Raw message received on {msg.topic}: {raw_message}")
        
        message = json.loads(raw_message)
        value = message.get("value")
        command = "ON" if value == 1.0 else "OFF" if value == 0.0 else None
        
        if command:
            logging.info(f"Received from Ubidots: {command}")
            local_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
            local_client.connect(CONFIG["local_mqtt"]["broker"], CONFIG["local_mqtt"]["port"], 60)
            local_client.publish(CONFIG["local_mqtt"]["alarm_topic"], command, qos=1)
            local_client.disconnect()
            logging.info(f"Command forwarded to {CONFIG['local_mqtt']['alarm_topic']}: {command}")
        else:
            logging.warning(f"Invalid value received from Ubidots: {message}")
    except Exception as e:
        logging.error(f"Error processing Ubidots message: {e}")

def send_data_periodically(local_client):
    """Periodically send latest data to Ubidots and check for alerts."""
    while True:
        try:
            result = get_latest_data()
            if result:
                temperature, humidity, ethylene, alarm = result
                logging.info(f"Latest data: temperature={temperature}, humidity={humidity}, ethylene={ethylene}, alarm={alarm}")
                
                send_to_ubidots(temperature, humidity, ethylene, alarm)
                
                if ethylene > CONFIG["ethylene_threshold"]:
                    notification = process_gemini_alert(ethylene, temperature, humidity)
                    send_gemini_message_to_ubidots(notification)
                    logging.info(f"Periodic alert: Ethylene threshold exceeded! Notification: {notification}")
                    local_client.publish(CONFIG["local_mqtt"]["notification_topic"], notification, qos=1)
                    logging.info(f"Notification published to {CONFIG['local_mqtt']['notification_topic']}")
                    local_client.publish(CONFIG["local_mqtt"]["alarm_topic"], "ON", qos=1)
                    logging.info(f"Alarm ON published to {CONFIG['local_mqtt']['alarm_topic']}")
            else:
                logging.info("No data in database.")
                
        except Exception as e:
            logging.error(f"Error in periodic data send: {e}")
        
        time.sleep(CONFIG["send_interval"])

def main():
    """Main function to initialize and run the MQTT sensor monitor."""
    local_client = None
    ubidots_client = None
    try:
        init_db()
        
        local_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        local_client.on_connect = on_connect_local
        local_client.on_message = on_message_local
        
        logging.info("Connecting to local MQTT broker...")
        local_client.connect(CONFIG["local_mqtt"]["broker"], CONFIG["local_mqtt"]["port"], 60)
        local_client.loop_start()
        
        ubidots_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        ubidots_client.username_pw_set(CONFIG["ubidots"]["token"])
        ubidots_client.on_connect = on_connect_ubidots
        ubidots_client.on_message = on_message_ubidots
        
        logging.info("Connecting to Ubidots MQTT broker...")
        ubidots_client.connect(CONFIG["ubidots"]["broker"], 1883, 60)
        ubidots_client.loop_start()
        
        threading.Thread(target=send_data_periodically, args=(local_client,), daemon=True).start()
        
        while True:
            time.sleep(1)
    except Exception as e:
        logging.error(f"Main loop error: {e}")
        raise
    finally:
        if local_client:
            local_client.loop_stop()
        if ubidots_client:
            ubidots_client.loop_stop()

if __name__ == "__main__":
    main()

