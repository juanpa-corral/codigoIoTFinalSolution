#include <Arduino.h>
#include <WiFi.h>
#include <ESPAsyncWebServer.h>
#include <SPIFFS.h>
#include <PubSubClient.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <DHT.h>

// -------------------- Configuración de pines y sensores ---------------------
#define ONE_WIRE_BUS 4        // DS18B20
#define DHTPIN 14             // DHT11
#define DHTTYPE DHT11
#define PIN_BUZZER 27
#define GAS_SENSOR_PIN 35     // Entrada analógica para sensor de gas (MQ-3 para etileno)
#define DEVICE_DISCONNECTED_C -127.0
#define ETHYLENE_THRESHOLD 0.80 // Umbral de etileno en ppm

OneWire oneWire(ONE_WIRE_BUS);
DallasTemperature ds18b20(&oneWire);
DHT dht(DHTPIN, DHTTYPE);

// -------------------- Configuración de red y MQTT ---------------------
const char* ssid = "Santiago's S24";
const char* password = "87654321";
const char* mqtt_server = "192.168.89.140"; // IP del Raspberry Pi con Mosquitto
const int mqtt_port = 1883;
const char* mqtt_client_id = "ESP32_Fruit_Monitor";
const char* mqtt_data_topic = "sensors/data";
const char* mqtt_alarm_topic = "sensors/alarm";

// -------------------- Variables globales ---------------------
float temperaturaDS18B20 = 0.0;
float humedadDHT11 = 0.0;
float ethylenePPM = 0.0;
int alarmState = 0;

// -------------------- Temporizadores ---------------------
unsigned long ultimoIntentoMQTT = 0;
const unsigned long intervaloMQTT = 5000;
unsigned long ultimoIntentoConexion = 0;
const unsigned long intervaloReconexion = 60000;
const unsigned long INTERVALO_PUBLICACION_MQTT = 10000; // 10 segundos
unsigned long ultimaPublicacionMQTT = 0;

// -------------------- Clientes ---------------------
AsyncWebServer server(80);
WiFiClient espClient;
PubSubClient client(espClient);

// -------------------- WiFi ---------------------
void checkWiFiConnection() {
  if (WiFi.status() != WL_CONNECTED) {
    Serial.println("[WiFi] Desconectado. Reintentando...");
    if (millis() - ultimoIntentoConexion >= intervaloReconexion) {
      WiFi.disconnect(true);
      WiFi.mode(WIFI_OFF);
      delay(1000);
      WiFi.mode(WIFI_STA);
      WiFi.begin(ssid, password);
      unsigned long startAttemptTime = millis();
      while (WiFi.status() != WL_CONNECTED && millis() - startAttemptTime < 15000) {
        delay(500);
        Serial.print(".");
      }
      ultimoIntentoConexion = millis();
      if (WiFi.status() == WL_CONNECTED) {
        Serial.println("\n[WiFi] Conectado!");
        Serial.print("[WiFi] IP: ");
        Serial.println(WiFi.localIP());
      } else {
        Serial.println("\n[WiFi] Error de conexión.");
      }
    }
  }
}

// -------------------- MQTT ---------------------
void intentarReconectarMQTT() {
  if (!client.connected() && millis() - ultimoIntentoMQTT > intervaloMQTT) {
    Serial.println("[MQTT] Intentando conectar...");
    if (client.connect(mqtt_client_id)) {
      Serial.println("[MQTT] Conectado al broker.");
      client.subscribe(mqtt_alarm_topic);
    } else {
      Serial.print("[MQTT] Falló conexión. Código: ");
      Serial.println(client.state());
    }
    ultimoIntentoMQTT = millis();
  }
}

void callback(char* topic, byte* payload, unsigned int length) {
  String message;
  for (int i = 0; i < length; i++) {
    message += (char)payload[i];
  }
  if (String(topic) == mqtt_alarm_topic) {
    if (message == "OFF") {
      digitalWrite(PIN_BUZZER, LOW);
      alarmState = 0;
      Serial.println("[MQTT] Alarma desactivada.");
    } else if (message == "ON") {
      digitalWrite(PIN_BUZZER, HIGH);
      alarmState = 1;
      Serial.println("[MQTT] Alarma activada.");
    } else {
      // Manejar mensajes de alerta de texto (enviados por Gemini API)
      Serial.println("[MQTT] Alerta recibida: " + message);
      digitalWrite(PIN_BUZZER, HIGH);
      alarmState = 1;
      // Apagar la alarma después de 5 segundos
      delay(5000);
      digitalWrite(PIN_BUZZER, LOW);
      alarmState = 0;
      client.publish(mqtt_alarm_topic, "OFF");
    }
  }
}

// -------------------- Tarea: Lectura de sensores ---------------------
void tareaLecturaSensores(void *parameter) {
  for (;;) {
    // Leer temperatura (DS18B20)
    ds18b20.requestTemperatures();
    float temp = ds18b20.getTempCByIndex(0);
    if (temp == DEVICE_DISCONNECTED_C) {
      Serial.println("[DS18B20] Sensor no detectado.");
    } else {
      temperaturaDS18B20 = temp;
      Serial.print("Temperatura (DS18B20): ");
      Serial.print(temperaturaDS18B20);
      Serial.println(" °C");
    }

    // Leer humedad (DHT11)
    humedadDHT11 = dht.readHumidity();
    if (isnan(humedadDHT11)) {
      Serial.println("[DHT11] Error al leer la humedad.");
    } else {
      Serial.print("Humedad (DHT11): ");
      Serial.print(humedadDHT11);
      Serial.println(" %");
    }

    // Leer sensor de gas (MQ-3 para etileno)
    int valorGas = analogRead(GAS_SENSOR_PIN);
    // Convertir a PPM (ajustar según calibración del sensor MQ-3)
    ethylenePPM = (valorGas / 4095.0) * 2.0; // Ejemplo: escalar a 0-2 ppm
    Serial.print("Etileno (MQ-3): ");
    Serial.print(ethylenePPM);
    Serial.println(" ppm");

    // Activar buzzer si el etileno excede el umbral
if (ethylenePPM <= ETHYLENE_THRESHOLD && alarmState == 1) {
  digitalWrite(PIN_BUZZER, LOW);
  alarmState = 0;
  Serial.println("[Buzzer] Desactivado por nivel normal de etileno.");
  if (client.connected()) {
    client.publish(mqtt_alarm_topic, "OFF");
    Serial.println("[MQTT] 1 Publicado: Alarma OFF por etileno.");
  }
}

    vTaskDelay(pdMS_TO_TICKS(3000));
  }
}

// -------------------- Tarea: Comunicaciones (WiFi y MQTT) ---------------------
void tareaComunicaciones(void *parameter) {
  for (;;) {
    checkWiFiConnection();
    intentarReconectarMQTT();
    client.loop();

    unsigned long ahora = millis();
    if (ahora - ultimaPublicacionMQTT >= INTERVALO_PUBLICACION_MQTT) {
      ultimaPublicacionMQTT = ahora;

      // Crear JSON con los datos
      String payload = "{";
      payload += "\"temperature\":" + String(temperaturaDS18B20, 2) + ",";
      payload += "\"humidity\":" + String(humedadDHT11, 2) + ",";
      payload += "\"ethylene\":" + String(ethylenePPM, 2) + ",";
      payload += "\"alarm\":" + String(alarmState);
      payload += "}";

      // Publicar JSON
      if (client.connected()) {
        client.publish(mqtt_data_topic, payload.c_str());
        Serial.println("[MQTT] Publicado JSON: " + payload);
      } else {
        Serial.println("[MQTT] No conectado, no se pudo publicar.");
      }
    }

    vTaskDelay(pdMS_TO_TICKS(1000));
  }
}

// -------------------- Setup ---------------------
void setup() {
  Serial.begin(9600);

  // Inicializar sensores
  dht.begin();
  ds18b20.begin();

  // Configurar pines
  pinMode(PIN_BUZZER, OUTPUT);
  pinMode(GAS_SENSOR_PIN, INPUT);

  // Inicializar SPIFFS
  if (!SPIFFS.begin(true)) {
    Serial.println("Error al montar SPIFFS");
    return;
  }

  // Conectar a WiFi
  WiFi.begin(ssid, password);
  Serial.print("[WiFi] Conectando...");
  unsigned long tInicio = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - tInicio < 20000) {
    delay(500);
    Serial.print(".");
  }
  if (WiFi.status() == WL_CONNECTED) {
    Serial.println("\n[WiFi] Conectado.");
    Serial.print("[WiFi] IP: ");
    Serial.println(WiFi.localIP());
  } else {
    Serial.println("\n[WiFi] No se pudo conectar.");
  }

  // Configurar MQTT
  client.setServer(mqtt_server, mqtt_port);
  client.setCallback(callback);

  // -------------------- Servidor Web ---------------------
  server.on("/", HTTP_GET, [](AsyncWebServerRequest *request) {
    request->send(SPIFFS, "/index.html", "text/html");
  });

  server.on("/disableAlarm", HTTP_GET, [](AsyncWebServerRequest *request) {
    digitalWrite(PIN_BUZZER, LOW);
    alarmState = 0;
    client.publish(mqtt_alarm_topic, "OFF");
    request->send(200, "text/plain", "Alarma desactivada");
  });

  server.on("/enableAlarm", HTTP_GET, [](AsyncWebServerRequest *request) {
    digitalWrite(PIN_BUZZER, HIGH);
    alarmState = 1;
    client.publish(mqtt_alarm_topic, "ON");
    request->send(200, "text/plain", "Alarma activada");
  });

  server.begin();

  // -------------------- Tareas en núcleos ---------------------
  xTaskCreatePinnedToCore(tareaLecturaSensores, "TareaSensores", 2048, NULL, 1, NULL, 1);
  xTaskCreatePinnedToCore(tareaComunicaciones, "TareaComunicacion", 4096, NULL, 1, NULL, 0);
}

// -------------------- Loop vacío ---------------------
void loop() {
  // Todo está en tareas
}