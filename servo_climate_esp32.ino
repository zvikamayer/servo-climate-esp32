// ESP32-S3 - בקרת סרוו (לחיצה לזווית נבחרת + חזרה אוטומטית) + תחנת מזג אוויר (DHT11)
// חיווט:
//   סרוו (למשל SG90)  - חוט אות -> GPIO6, חוט (+) -> 5V, חוט (-) -> GND
//   DHT11             - VCC -> 3.3V, GND -> GND, DATA -> GPIO4
//                        (אם המודול שלך הוא חיישן "עירום" בלי לוחית - צריך גם
//                        נגד משיכה 10kΩ בין DATA ל-VCC; רוב המודולים המוכנים
//                        (3 פינים על לוחית קטנה) כבר כוללים אותו על הלוח)
//
// לוגיקה סרוו: באתר בוחרים זווית עם סליידר ולוחצים "הפעל" - הסרוו נע בתנועה
// חלקה לזווית שנבחרה, נעצר שם לרגע קצר (HOLD_MS), וחוזר לבד לזווית ההתחלה
// (REST_ANGLE) - בדיוק כמו לחיצה על כפתור. כל לחיצה נרשמת ביומן (שעה+תאריך,
// לפי שעון מסונכרן מהאינטרנט) ונשמרת ב-NVS (שורדת אתחול). אי אפשר להתחיל
// לחיצה חדשה בזמן שקודמת עדיין בתהליך - נמנעים מלהפריע לתנועה באמצע.
//
// לוגיקה מזג אוויר: קריאת טמפ'/לחות כל כמה שניות לתצוגה חיה באתר, ושמירת
// נקודת מדידה אחת לשעה (HISTORY_INTERVAL_MS) ב-NVS לצורך גרף היסטוריה
// באתר (ראה MAX_HISTORY_POINTS - כמה שעות אחורה נשמרות).
//
// עדכון חי: בדיוק כמו שאר הפרויקטים - MQTT ציבורי (broker.hivemq.com),
// בלי טוקן/סיסמה/חשבון. האתר שולח פקודות והבקר מבצע אותן מיד ומפרסם סטטוס.

#include <WiFi.h>
#include <WiFiMulti.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <ESP32Servo.h>
#include <DHT.h>
#include <time.h>
#include <Preferences.h>
#include "secrets.h"

// ---------- הגדרות חומרה ----------
#define SERVO_PIN   6     // GPIO6 - אות לסרוו (כמו בפרויקט ac_pump_esp32, מאומת על S3)
#define DHT_PIN     4     // GPIO4 - DATA של ה-DHT11 (כמו בפרויקט led-scheduler)
#define DHT_TYPE    DHT11

// ---------- סרוו ----------
#define REST_ANGLE        0      // מצב התחלתי - לשם הסרוו חוזר אוטומטית אחרי כל לחיצה
#define HOLD_MS            800   // כמה זמן להישאר בזווית שנבחרה לפני החזרה (מדמה "לחיצת כפתור")
#define SERVO_RATE_SEC     0.6f  // שניות ל-90 מעלות - קצב התנועה החלקה (כמו בפרויקט servo_timer_esp32)
#define SERVO_UPDATE_MS    20    // כל כמה זמן לעדכן את הזווית בפועל בזמן תנועה
#define MAX_LOG_ENTRIES    30    // כמה לחיצות אחרונות שומרים ומפרסמים

// ---------- מזג אוויר ----------
#define DHT_READ_INTERVAL_MS   2000UL             // כל כמה זמן לקרוא את החיישן לתצוגה חיה
#define HISTORY_INTERVAL_MS    (60UL * 60UL * 1000UL) // שעה - כל כמה זמן שומרים נקודה להיסטוריה/גרף
                                                        // (לבדיקה מהירה אפשר להקטין זמנית, למשל ל-60000UL לדקה)
#define MAX_HISTORY_POINTS     48                 // 48 שעות (יומיים) - הגדלה אפשרית, אבל מגדילה את הודעת ה-MQTT

// ---------- וויפי / MQTT ----------
#define WIFI_RETRY_MS       15000
#define MQTT_BROKER         "broker.hivemq.com"
#define MQTT_PORT           1883
#define MQTT_TOPIC_STATUS   "zvikamayer-servoclimate/status"
#define MQTT_TOPIC_CMD      "zvikamayer-servoclimate/command"
#define MQTT_TOPIC_INFO     "zvikamayer-servoclimate/info"
#define MQTT_RETRY_MS       5000
#define STATUS_INTERVAL_MS  10000  // פרסום סטטוס תקופתי (גם בלי שינוי) - כדי שהאתר תמיד יראה נתונים טריים
#define HEARTBEAT_MS        5000

struct PressEvent {
  uint32_t id;
  uint32_t epoch;   // 0 = השעה לא ידועה (השעון לא היה מסונכרן באותו רגע)
  uint8_t  angle;
};

struct ClimatePoint {
  uint32_t epoch;
  int16_t  tempX10;  // טמפרטורה * 10 (עשירית מעלה), כדי לשמור כמספר שלם קומפקטי
  int16_t  humX10;   // לחות * 10
};

enum PressState { PRESS_IDLE, PRESS_SWEEP_TO_TARGET, PRESS_HOLDING, PRESS_SWEEP_TO_REST };

Preferences prefs;
WiFiMulti wifiMulti;
WiFiClient mqttNetClient;
PubSubClient mqttClient(mqttNetClient);
unsigned long lastMqttAttempt = 0;
unsigned long lastWifiAttempt = 0;
bool wasWifiConnected = false;
bool timeSynced = false;

Servo servo;
int servoAngle = REST_ANGLE;
bool servoSweeping = false;
int servoSweepStartAngle = 0;
int servoSweepTargetAngle = 0;
unsigned long servoSweepStartMillis = 0;
unsigned long servoSweepDurationMs = 0;
unsigned long lastServoUpdate = 0;

PressState pressState = PRESS_IDLE;
unsigned long holdStartMillis = 0;

PressEvent pressLog[MAX_LOG_ENTRIES];
int logCount = 0;
uint32_t nextLogId = 1;

DHT dht(DHT_PIN, DHT_TYPE);
float temperature = NAN;
float humidity = NAN;
unsigned long lastDhtRead = 0;

ClimatePoint history[MAX_HISTORY_POINTS];
int historyCount = 0;

unsigned long lastStatusPublish = 0;
bool statusDirty = true;
unsigned long lastHeartbeat = 0;

// ---------- יומן לחיצות (NVS) ----------

void loadPressLog() {
  prefs.begin("presslog", true);
  logCount = prefs.getInt("count", 0);
  if (logCount > MAX_LOG_ENTRIES) logCount = MAX_LOG_ENTRIES;
  prefs.getBytes("entries", pressLog, sizeof(PressEvent) * logCount);
  nextLogId = prefs.getUInt("nextId", 1);
  prefs.end();
}

void savePressLog() {
  prefs.begin("presslog", false);
  prefs.putInt("count", logCount);
  prefs.putBytes("entries", pressLog, sizeof(PressEvent) * logCount);
  prefs.putUInt("nextId", nextLogId);
  prefs.end();
}

void addPressLogEntry(uint32_t epoch, uint8_t angle) {
  if (logCount >= MAX_LOG_ENTRIES) {
    memmove(&pressLog[0], &pressLog[1], sizeof(PressEvent) * (MAX_LOG_ENTRIES - 1));
    logCount = MAX_LOG_ENTRIES - 1;
  }
  pressLog[logCount].id = nextLogId++;
  pressLog[logCount].epoch = epoch;
  pressLog[logCount].angle = angle;
  logCount++;
  savePressLog();
}

void deletePressLogEntry(uint32_t id) {
  for (int i = 0; i < logCount; i++) {
    if (pressLog[i].id == id) {
      memmove(&pressLog[i], &pressLog[i + 1], sizeof(PressEvent) * (logCount - i - 1));
      logCount--;
      savePressLog();
      return;
    }
  }
}

// ---------- היסטוריית מזג אוויר (NVS) ----------

void loadHistory() {
  prefs.begin("climhist", true);
  historyCount = prefs.getInt("count", 0);
  if (historyCount > MAX_HISTORY_POINTS) historyCount = MAX_HISTORY_POINTS;
  prefs.getBytes("points", history, sizeof(ClimatePoint) * historyCount);
  prefs.end();
}

void saveHistory() {
  prefs.begin("climhist", false);
  prefs.putInt("count", historyCount);
  prefs.putBytes("points", history, sizeof(ClimatePoint) * historyCount);
  prefs.end();
}

void addHistoryPoint(uint32_t epoch, int16_t tempX10, int16_t humX10) {
  if (historyCount >= MAX_HISTORY_POINTS) {
    memmove(&history[0], &history[1], sizeof(ClimatePoint) * (MAX_HISTORY_POINTS - 1));
    historyCount = MAX_HISTORY_POINTS - 1;
  }
  history[historyCount].epoch = epoch;
  history[historyCount].tempX10 = tempX10;
  history[historyCount].humX10 = humX10;
  historyCount++;
  saveHistory();
}

uint32_t lastHistoryEpoch() {
  return historyCount > 0 ? history[historyCount - 1].epoch : 0;
}

// ---------- וויפי ושעון ----------

void trySyncTime() {
  configTime(2 * 3600, 3600, "pool.ntp.org", "time.google.com"); // ישראל: UTC+2 קבוע + שעון קיץ אוטומטי (DST)
  struct tm timeinfo;
  if (getLocalTime(&timeinfo, 3000)) {
    timeSynced = true;
    Serial.println("שעון סונכרן מהאינטרנט.");
  }
}

void handleWiFi() {
  bool connected = (WiFi.status() == WL_CONNECTED);
  if (connected && !wasWifiConnected) {
    Serial.print("[WiFi] מחובר. IP=");
    Serial.println(WiFi.localIP());
  } else if (!connected && wasWifiConnected) {
    Serial.println("[WiFi] החיבור אבד.");
  }
  wasWifiConnected = connected;

  if (connected) {
    if (!timeSynced) trySyncTime();
    return;
  }
  unsigned long now = millis();
  if (now - lastWifiAttempt < WIFI_RETRY_MS) return;
  lastWifiAttempt = now;
  wifiMulti.run();
}

// ---------- סרוו: תנועה חלקה מבוססת-קצב ----------

// מתחילה תנועה חלקה לזווית היעד. משך התנועה נגזר מ-SERVO_RATE_SEC (שניות
// ל-90 מעלות) לפי גודל הקפיצה בפועל, כך שקצב הסיבוב (מעלות/שנייה) קבוע.
void startServoSweep(int targetAngle) {
  targetAngle = constrain(targetAngle, 0, 180);
  if (targetAngle == servoAngle) {
    servoSweeping = false;
    return;
  }
  servoSweepStartAngle = servoAngle;
  servoSweepTargetAngle = targetAngle;
  servoSweepStartMillis = millis();
  int delta = abs(servoSweepTargetAngle - servoSweepStartAngle);
  servoSweepDurationMs = (unsigned long)(SERVO_RATE_SEC * 1000.0f * delta / 90.0f);
  if (servoSweepDurationMs < 20) servoSweepDurationMs = 20;
  servoSweeping = true;
}

void updateServoSweep() {
  if (!servoSweeping) return;
  unsigned long now = millis();
  if (now - lastServoUpdate < SERVO_UPDATE_MS) return;
  lastServoUpdate = now;

  unsigned long elapsed = now - servoSweepStartMillis;
  if (elapsed >= servoSweepDurationMs) {
    servoAngle = servoSweepTargetAngle;
    servoSweeping = false;
    servo.write(servoAngle);

    // התנועה הנוכחית הגיעה ליעד - ממשיכים למצב הבא בתהליך הלחיצה (אם רלוונטי)
    if (pressState == PRESS_SWEEP_TO_TARGET) {
      pressState = PRESS_HOLDING;
      holdStartMillis = millis();
    } else if (pressState == PRESS_SWEEP_TO_REST) {
      pressState = PRESS_IDLE;
    }
    statusDirty = true;
  } else {
    float t = (float)elapsed / (float)servoSweepDurationMs;
    servoAngle = servoSweepStartAngle + (int)round((servoSweepTargetAngle - servoSweepStartAngle) * t);
    servo.write(servoAngle);
  }
}

// מתחילה שלב תנועה בתהליך הלחיצה. אם היעד זהה לזווית הנוכחית (למשל נבחרה
// זווית ההתחלה עצמה) - startServoSweep לא מזיז כלום ולא תהיה "הגעה" דרך
// updateServoSweep, אז עוברים למצב הבא מיד כאן במקום להיתקע.
void beginPressSweep(int targetAngle, PressState duringState, PressState immediateAfterState) {
  startServoSweep(targetAngle);
  if (servoSweeping) {
    pressState = duringState;
  } else {
    pressState = immediateAfterState;
    if (immediateAfterState == PRESS_HOLDING) holdStartMillis = millis();
  }
}

void updatePressState() {
  if (pressState == PRESS_HOLDING && millis() - holdStartMillis >= HOLD_MS) {
    beginPressSweep(REST_ANGLE, PRESS_SWEEP_TO_REST, PRESS_IDLE);
    statusDirty = true;
  }
}

void handlePressCommand(int angle) {
  if (pressState != PRESS_IDLE) {
    Serial.println("[לחיצה] כבר בתהליך - מתעלם מהפקודה החדשה.");
    return;
  }
  angle = constrain(angle, 0, 180);
  uint32_t epoch = timeSynced ? (uint32_t)time(nullptr) : 0;
  addPressLogEntry(epoch, (uint8_t)angle);
  beginPressSweep(angle, PRESS_SWEEP_TO_TARGET, PRESS_HOLDING);
  statusDirty = true;
  Serial.print("[לחיצה] יעד: ");
  Serial.print(angle);
  Serial.println("°");
}

// ---------- מזג אוויר ----------

void maybeRecordHistory() {
  if (!timeSynced || isnan(temperature) || isnan(humidity)) return;
  uint32_t nowEpoch = (uint32_t)time(nullptr);
  uint32_t last = lastHistoryEpoch();
  if (last != 0 && nowEpoch - last < (HISTORY_INTERVAL_MS / 1000UL)) return;
  addHistoryPoint(nowEpoch, (int16_t)round(temperature * 10), (int16_t)round(humidity * 10));
  statusDirty = true;
  Serial.println("[היסטוריה] נשמרה נקודה חדשה לגרף.");
}

void readClimate() {
  unsigned long now = millis();
  if (now - lastDhtRead < DHT_READ_INTERVAL_MS) return;
  lastDhtRead = now;

  float t = dht.readTemperature();
  float h = dht.readHumidity();
  if (!isnan(t)) temperature = t;
  if (!isnan(h)) humidity = h;

  maybeRecordHistory();
}

// ---------- MQTT ----------

void mqttCallback(char *topic, byte *payload, unsigned int length) {
  String msg;
  for (unsigned int i = 0; i < length; i++) msg += (char)payload[i];
  msg.trim();

  if (msg.startsWith("press:")) {
    handlePressCommand(msg.substring(6).toInt());
  } else if (msg.startsWith("delete:")) {
    deletePressLogEntry((uint32_t)msg.substring(7).toInt());
    statusDirty = true;
  } else {
    Serial.println("[MQTT] פקודה לא מזוהה - התעלמות.");
  }
}

void connectMqttIfNeeded() {
  if (WiFi.status() != WL_CONNECTED) return;
  if (mqttClient.connected()) {
    mqttClient.loop();
    return;
  }
  unsigned long now = millis();
  if (now - lastMqttAttempt < MQTT_RETRY_MS) return;
  lastMqttAttempt = now;

  char clientId[24];
  snprintf(clientId, sizeof(clientId), "climate-%06x", (uint32_t)ESP.getEfuseMac());
  if (mqttClient.connect(clientId, NULL, NULL, MQTT_TOPIC_INFO, 1, true, "{\"online\":false}")) {
    mqttClient.subscribe(MQTT_TOPIC_CMD);
    Serial.println("MQTT מחובר.");
    char infoMsg[96];
    snprintf(infoMsg, sizeof(infoMsg), "{\"online\":true,\"ip\":\"%s\"}", WiFi.localIP().toString().c_str());
    mqttClient.publish(MQTT_TOPIC_INFO, infoMsg, true);
    statusDirty = true;
  } else {
    Serial.print("MQTT נכשל, קוד: ");
    Serial.println(mqttClient.state());
  }
}

void publishStatus() {
  if (!mqttClient.connected()) return;
  JsonDocument doc;

  bool climateOk = !isnan(temperature) && !isnan(humidity);
  doc["climateOk"] = climateOk;
  doc["temp"] = climateOk ? roundf(temperature * 10) / 10.0f : 0;
  doc["hum"] = climateOk ? roundf(humidity * 10) / 10.0f : 0;

  doc["servoAngle"] = servoAngle;
  doc["servoSweeping"] = servoSweeping;
  doc["pressState"] = (int)pressState; // 0=מוכן 1=נע ליעד 2=עוצר 3=חוזר להתחלה
  doc["restAngle"] = REST_ANGLE;

  doc["timeSynced"] = timeSynced;
  doc["nowEpoch"] = timeSynced ? (uint32_t)time(nullptr) : 0;

  JsonArray histArr = doc["history"].to<JsonArray>();
  for (int i = 0; i < historyCount; i++) {
    JsonObject e = histArr.add<JsonObject>();
    e["t"] = history[i].epoch;
    e["c"] = history[i].tempX10;
    e["h"] = history[i].humX10;
  }

  JsonArray logArr = doc["log"].to<JsonArray>();
  for (int i = 0; i < logCount; i++) {
    JsonObject e = logArr.add<JsonObject>();
    e["id"] = pressLog[i].id;
    e["t"] = pressLog[i].epoch;
    e["a"] = pressLog[i].angle;
  }

  String payload;
  serializeJson(doc, payload);
  bool ok = mqttClient.publish(MQTT_TOPIC_STATUS, payload.c_str(), true); // retained
  if (!ok) {
    Serial.print("MQTT פרסום נכשל, אורך הודעה: ");
    Serial.println(payload.length());
  }
}

void setup() {
  Serial.begin(115200);

  servo.attach(SERVO_PIN);
  servoAngle = REST_ANGLE;
  servo.write(servoAngle);

  dht.begin();

  loadPressLog();
  loadHistory();

  WiFi.mode(WIFI_STA);
  wifiMulti.addAP(WIFI_SSID, WIFI_PASSWORD);
  wifiMulti.addAP(WIFI_SSID_2, WIFI_PASSWORD_2);
  wifiMulti.addAP(WIFI_SSID_3, WIFI_PASSWORD_3);
  wifiMulti.addAP(WIFI_SSID_4, WIFI_PASSWORD_4);
  wifiMulti.run();

  mqttClient.setBufferSize(6144); // ההיסטוריה+היומן יכולים לגדול, ברירת המחדל (256) קטנה מדי
  mqttClient.setServer(MQTT_BROKER, MQTT_PORT);
  mqttClient.setCallback(mqttCallback);

  Serial.println("מוכן. ממתין לחיבור וויפי/MQTT ולפקודות מהאתר.");
}

void loop() {
  handleWiFi();
  connectMqttIfNeeded();

  updateServoSweep();
  updatePressState();
  readClimate();

  unsigned long now = millis();
  if (statusDirty || (now - lastStatusPublish >= STATUS_INTERVAL_MS)) {
    publishStatus();
    statusDirty = false;
    lastStatusPublish = now;
  }

  if (now - lastHeartbeat >= HEARTBEAT_MS) {
    lastHeartbeat = now;
    Serial.print("[מצב] WiFi=");
    Serial.print(wasWifiConnected ? WiFi.localIP().toString() : String("מנותק"));
    Serial.print("  MQTT=");
    Serial.print(mqttClient.connected() ? "מחובר" : "מנותק");
    Serial.print("  טמפ'=");
    Serial.print(isnan(temperature) ? -99 : temperature);
    Serial.print("  לחות=");
    Serial.print(isnan(humidity) ? -99 : humidity);
    Serial.print("  סרוו=");
    Serial.print(servoAngle);
    Serial.print("°  מצב לחיצה=");
    Serial.println(pressState);
  }
}
