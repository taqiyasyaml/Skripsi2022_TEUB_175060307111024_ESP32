#include <FS.h>
#include <SPIFFS.h> //ESP32
#include <WiFi.h> //ESP32
#include <WebServer.h> //ESP32
#include <ArduinoWebsockets.h> //GilMaimon
#include <ArduinoJson.h> //ArduinoJson.org

const uint8_t adc_read = 34;
const uint8_t rdy = 15;
const uint8_t ale = 4;
const uint16_t t_pd_us = 10;
const uint8_t wifi_switch_button = 0;
const uint8_t wifi_indicator_led = 2;
const uint8_t ai0_a3_d_write [] = {26, 27, 14, 13};
const uint8_t d_read = 35;
const uint16_t t_pd_readadc_ms = 2;
const uint8_t max_try_write = 10;
const uint8_t max_try_read = 11;
const bool can_read_state = true;
const bool format_spiffs_begin_error = true;
const uint16_t timeout_wifi_ws_connect_s = 30;
const uint16_t try_wifi_ws_connect_s = 5;
const uint16_t timeout_switch_wifi_button_s = 3;
const uint16_t timeout_ws_ping_s = 15;
const uint16_t timeout_ws_ping_priority_s = 5;

struct {
  unsigned long t_s;
  unsigned long last_millis;
} data_clk;
struct {
  bool can_read;
  uint16_t margin_adc;
  unsigned long margin_t_ms;
  uint16_t adc_val;
  uint16_t last_adc_sent;
  unsigned long last_ms_sent;
  bool line_state[8];
  bool line_broken[8];
} data_io[16];
struct WLConfig {
  String ssid;
  String ssid_password;
  String ws_url;
};

WLConfig wl_config;
DynamicJsonDocument req_json(65536);
bool spiffs_ready=false;
uint8_t io_loop_update_json=0;
unsigned long target_timeout_switch_wifi=0;

void setup_spiffs();
void setup_pin();
void setup_data_io();
void setup_broken_io_cant_read_state();
void setup_http_ws();

void loop_serial_json();
void loop_update_json();
void loop_http_ws_wifi();
void loop_switch_wl();
bool loop_ping();

void blink_led(unsigned long dly_ms,bool toggle);

void set_clk(unsigned long clk);
unsigned long get_clk();

bool get_wl_config();
void disconnect_wl();
void setup_wifi();
void connect_ws();
void setup_ap();
WebServer http_server(80);
void index_post_handler();
using namespace websockets;
WebsocketsClient ws_client;
void ws_on_message(WebsocketsMessage message);
void ws_on_other(WebsocketsEvent e, String str_msg);

void clear_ale_rdy();
bool write_address (uint8_t val);
bool send_a_i_d (uint8_t val);
bool write_iset_d (
  uint8_t val, bool d, 
  unsigned long before_us, 
  unsigned long toggle_us,
  unsigned long after_us
);
bool write_iset (uint8_t val) { return write_iset_d (val,false,0,0,0); }
bool read_d_iset (uint8_t val);
uint16_t read_adc_iset (uint8_t val);

bool set_io (uint8_t io);
bool set_io_line (uint8_t io, uint8_t line);
bool write_io_line (
  uint8_t io, uint8_t line, bool d,
  unsigned long before_us, 
  unsigned long toggle_us,
  unsigned long after_us
);
bool read_io_line (uint8_t io, uint8_t line);
uint16_t read_adc_io (uint8_t io);
bool reset_io(uint8_t io);
void reset_cs();

unsigned long last_req_json_ws_ping = 0;

bool req_json_get_state(bool from_ws);
bool req_json_set_states();
bool res_json_reply_sync_id(bool to_ws);
void res_json_set_state(bool to_ws);
bool req_json_get_adc(bool from_ws);
bool req_json_set_adc(bool from_ws);
bool req_json_read_adc(bool from_ws);
void res_json_set_adc(bool to_ws);
bool req_json_rst_io();
bool req_json_rst_cs();
void req_json_rst_all();
void req_json_duplicate_block();
bool req_json_ws_ping();
bool req_json_ws_pong();
void req_json_ws_offline();
void res_json_err_req(bool to_ws);
void res_json_err_json(String msg_no_json);
void res_json_print(String str_print);
void route_req_json(bool from_ws);

void setup() {
  // put your setup code here, to run once:
  Serial.begin(115200);
  setup_spiffs();
  setup_pin();
  setup_data_io();
  if(!can_read_state) setup_broken_io_cant_read_state();
  setup_http_ws();
  setup_wifi();
}

void loop() {
  // put your main code here, to run repeatedly:
  if(loop_ping()) return;
  loop_serial_json();
  loop_update_json();
  loop_http_ws_wifi();
  loop_switch_wl();
}

void setup_pin(){
  pinMode(wifi_switch_button,INPUT);
  pinMode(wifi_indicator_led,OUTPUT);
  pinMode(rdy,OUTPUT);
  pinMode(ale,OUTPUT);
  clear_ale_rdy();
  for(uint8_t a=0;a<4;a++) pinMode(ai0_a3_d_write[a],OUTPUT);
  send_a_i_d(0);
  pinMode(d_read,INPUT_PULLDOWN);
}

void setup_data_io(){
  for(uint8_t io=0;io<16;io++){
    data_io[io].can_read = false;
    data_io[io].margin_adc = 0;
    data_io[io].margin_t_ms = 0;
    data_io[io].adc_val = 0;
    data_io[io].last_adc_sent = 0;
    data_io[io].last_ms_sent = 0;
    for(uint8_t line=0;line<8;line++){
      data_io[io].line_state[line]=false;
      data_io[io].line_broken[line]=false;
    }
  }
}
void setup_broken_io_cant_read_state(){
//  for(uint8_t io=2;io<=6;io++){
//    for(uint8_t line=0;line<8;line++)
//      data_io[io].line_broken[line]=true;
//  }  
//  for(uint8_t io=9;io<=14;io++){
//    for(uint8_t line=0;line<8;line++)
//      data_io[io].line_broken[line]=true;
//  }  
}
void setup_http_ws(){
  ws_client.onMessage(ws_on_message);
  ws_client.onEvent(ws_on_other);
  http_server.on("/",HTTP_POST,index_post_handler);
  http_server.on("/index.html",HTTP_POST,index_post_handler);
  http_server.on("/blink",[](){
    http_server.send(404,"text/plain","BLINK!");
    blink_led(500);
    digitalWrite(wifi_indicator_led,(WiFi.getMode()==WIFI_AP || WiFi.getMode()==WIFI_AP_STA)?HIGH:LOW);
  });
  if(!spiffs_ready)
    setup_spiffs();
  if(spiffs_ready){
    http_server.serveStatic("/",SPIFFS,"/index.html");
    http_server.serveStatic("/index.html",SPIFFS,"/index.html");
    http_server.serveStatic("/bootstrap.js",SPIFFS,"/bootstrap.js");
    http_server.serveStatic("/bootstrap.css",SPIFFS,"/bootstrap.css");
    http_server.serveStatic("/wl_config.json",SPIFFS,"/wl_config.json");
  }
  http_server.onNotFound([](){
    res_json_print("(setup_http_ws) "+String(http_server.uri())+" Not Found");
    if(spiffs_ready){
      File file404 = SPIFFS.open("/404.html",FILE_READ);
      if(!file404 || file404.isDirectory()){
        res_json_print("(setup_http_ws) 404 Template Not Found");
        http_server.send(404,"text/plain","Not Found");
      }else
        http_server.streamFile(file404,"text/html");
      file404.close();
    } else 
      http_server.send(404,"text/plain","Not Found");
  });
}
void setup_spiffs(){
  spiffs_ready=SPIFFS.begin(format_spiffs_begin_error);
  if(!spiffs_ready)
    res_json_print("SPIFFS begin failed");
}

void ws_on_message(WebsocketsMessage msg) {
  if(msg.isText()){
    DeserializationError err = deserializeJson(req_json,msg.data());
    if(err) res_json_err_json(msg.data());
    else route_req_json(true);
    blink_led(150,WiFi.getMode()==WIFI_AP);
    blink_led(150,WiFi.getMode()==WIFI_AP);
  }
}
void ws_on_other(WebsocketsEvent e, String str_msg){
  if(e == WebsocketsEvent::GotPong)
    res_json_ws_pong();
}
void index_post_handler(){
  res_json_print("(index_post_handler) Start Post Handler");
  int http_code = 200;
  get_wl_config();
  WLConfig old_config={wl_config.ssid,wl_config.ssid_password,wl_config.ws_url};
  if(http_server.hasArg("ssid")) wl_config.ssid = http_server.arg("ssid");
  if(http_server.hasArg("ssid_password")) wl_config.ssid_password = http_server.arg("ssid_password");
  if(http_server.hasArg("ws_url")) wl_config.ws_url = http_server.arg("ws_url");  
  if(spiffs_ready){
    if(!wl_config.ssid.equals("") && (wl_config.ws_url.indexOf("ws://")>=0 || wl_config.ws_url.indexOf("wss://")>=0)){
      DynamicJsonDocument json(350);
      json["ssid"]=wl_config.ssid;
      json["ssid_password"]=wl_config.ssid_password;
      json["ws_url"]=wl_config.ws_url;
      File f = SPIFFS.open("/wl_config.json",FILE_WRITE);
      if(!f || f.isDirectory()){
        res_json_print("(index_post_handler) Cant write/wl_config.json");
        http_code = 500;
      }else serializeJson(json,f);
      f.close();
    } else http_code = 400;
  } else http_code = 500;
  if(http_code == 200){
    if(get_wl_config()){
      File f200 = SPIFFS.open("/post_success.html",FILE_READ);
      if(!f200 || f200.isDirectory()) http_server.send(200,"text/plain","OK");
      else http_server.streamFile(f200,"text/html");
      f200.close();
      if(WiFi.getMode()==WIFI_AP || WiFi.getMode()==WIFI_AP_STA || !wl_config.ssid.equals(old_config.ssid) || !wl_config.ssid_password.equals(old_config.ssid_password)){
        res_json_print("(index_post_handler) Switch WiFi");
        delay(1000);
        setup_wifi();
      } else if (!wl_config.ws_url.equals(old_config.ws_url)){
        res_json_print("(index_post_handler) Switch WebSocket");
        connect_ws();
      }
    }else http_code = 500;
  }
  if(http_code != 200) {
    get_wl_config();
    if(!spiffs_ready) res_json_print("(index_post_handler) SPIFFS not Ready");
    else {
      File f = SPIFFS.open("/post_fail.html",FILE_READ);
      if(!f || f.isDirectory()){
        res_json_print("(index_post_handler) Post Fail Template Not Found");
        if(http_code == 400) http_server.send(400,"text/plain","Bad Request");
        else if(http_code == 500) http_server.send(500,"text/plain","Internal Server Error");
      }else http_server.streamFile(f,"text/html");
      f.close();
    }
  }
  res_json_print("(index_post_handler) Post Handler Done");
}
void loop_http_ws_wifi(){
  if(WiFi.getMode() == WIFI_STA || WiFi.getMode() == WIFI_AP_STA){
    if(WiFi.status() != WL_CONNECTED){
      res_json_print("(loop_http_ws_wifi) WiFi Disconnected !");
      setup_wifi();
    } else if(!ws_client.available()) {
      res_json_print("(loop_http_ws_wifi) WebSocket Disconnected !");
      connect_ws();
    } else
      ws_client.poll();
  } else if(ws_client.available()) {
    res_json_print("(loop_http_ws_wifi) Unknown WebSocket Connection");
    ws_client.close();  
  }
  if(WiFi.getMode() != WIFI_OFF)
    http_server.handleClient();
}
bool loop_ping(){
  if(last_req_json_ws_ping>0){
    if(millis() >= last_req_json_ws_ping+timeout_ws_ping_s*1000){
      res_json_print("(loop_ping) Ping Reponse Timeout");
      last_req_json_ws_ping=0;
      return false;
    }
    if(!ws_client.available()){
      last_req_json_ws_ping=0;
      res_json_print("(loop_ping) Unknown Ping");
      return false;
    } else ws_client.poll();
    return millis() <= last_req_json_ws_ping+timeout_ws_ping_priority_s*1000;
  }
  return false;
}
bool get_wl_config() {
  wl_config.ssid = "";
  wl_config.ssid_password = "";
  wl_config.ws_url = "";
  if (!spiffs_ready)
    setup_spiffs();
  if (spiffs_ready){
    File f = SPIFFS.open("/wl_config.json",FILE_READ);
    if(!f || f.isDirectory())
      res_json_print("(get_wl_config) /wl_config.json Not Found");
    else{
      DynamicJsonDocument json(400);
      DeserializationError error = deserializeJson(json, f);
      if(error)
        res_json_print("(get_wl_config)  can't parse JSON from /wl_config.json");
      else {
        JsonVariant ssid_var = json["ssid"];
        JsonVariant ssid_password_var = json["ssid_password"];
        JsonVariant ws_url_var = json["ws_url"];
        wl_config.ssid = ssid_var.is<String>()?ssid_var.as<String>():"";
        wl_config.ssid_password = ssid_password_var.is<String>()?ssid_password_var.as<String>():"";
        wl_config.ws_url = ws_url_var.is<String>()?ws_url_var.as<String>():"";
      }
    }
    f.close();
  }
  res_json_print("(get_wl_config) SSID : "+wl_config.ssid+" SSID Password Length : "+String(wl_config.ssid_password.length())+" URL WebSocket : "+wl_config.ws_url);
  return !wl_config.ssid.equals("") && (wl_config.ws_url.indexOf("ws://")>=0 || wl_config.ws_url.indexOf("wss://")>=0);
}

void disconnect_wl() {
  if (ws_client.available()){
    res_json_print("(disconnect_wl) Turn off WebSocket");
    ws_client.close();
  }
  if(WiFi.getMode() != WIFI_OFF){
    res_json_print("(disconnect_wl) Turn off HTTP Server");
    http_server.stop();
  }
  if(WiFi.getMode() == WIFI_AP || WiFi.getMode() == WIFI_AP_STA){
    res_json_print("(disconnect_wl) Turn off Access Point");
    WiFi.softAPdisconnect(true);
  }
  if((WiFi.getMode() == WIFI_STA || WiFi.getMode() == WIFI_AP_STA) && WiFi.status() == WL_CONNECTED) {
    res_json_print("(disconnect_wl) Turn off WiFi");
    WiFi.disconnect(true);
  }
}
void setup_ap() {
  disconnect_wl();
  WiFi.mode(WIFI_AP);
  const String ssid_ap = "CrossbarSwitch"+WiFi.softAPmacAddress();
  res_json_print("(setup_ap) Turn On SSID "+ssid_ap);
  WiFi.softAP(ssid_ap.c_str());
  res_json_print("(setup_ap) IP AP : "+WiFi.softAPIP().toString());
  http_server.begin();
  digitalWrite(wifi_indicator_led,HIGH);
}
void setup_wifi(){
  if(!get_wl_config()){
    res_json_print("(setup_wifi) Cant Turn On WiFi. Unknown Config.");
    setup_ap();
    return;
  }
  unsigned long timeout_wifi = millis() + timeout_wifi_ws_connect_s * 1000;
  unsigned long next_try_wifi = 0;
  bool first_switch_button = digitalRead(wifi_switch_button) == HIGH;
  disconnect_wl();
  WiFi.mode(WIFI_STA);
  res_json_print("(setup_wifi) Connect SSID : "+wl_config.ssid);
  WiFi.begin(wl_config.ssid.c_str(), wl_config.ssid_password.c_str());
  while(WiFi.status() != WL_CONNECTED && 
  ((millis() < timeout_wifi && ((first_switch_button == true && digitalRead(wifi_switch_button) == HIGH) || first_switch_button == false)) || (first_switch_button == false && digitalRead(wifi_switch_button) == LOW))
  ){
    if(millis()>=next_try_wifi){
      WiFi.begin(wl_config.ssid.c_str(), wl_config.ssid_password.c_str());
      next_try_wifi = millis()+try_wifi_ws_connect_s*1000;
    }
    blink_led(400);
    res_json_print("(setup_wifi) Try connect SSID");
    if(first_switch_button==false&&digitalRead(wifi_switch_button) == HIGH)
      first_switch_button==true;
  }
  if(WiFi.status() != WL_CONNECTED){
    res_json_print("(setup_wifi) Fail to connect SSID");
    setup_ap();
    while(first_switch_button==true&&digitalRead(wifi_switch_button) == LOW);
  }else {
    res_json_print("(setup_wifi) IP Client "+WiFi.localIP().toString());
    http_server.begin();
    connect_ws();
  }
}
void connect_ws() {
  if(!get_wl_config()){
    res_json_print("(connect_ws) Cant connect WebSocket. Unknown Config.");
    setup_ap();
    return;
  }
  if((WiFi.getMode() != WIFI_STA && WiFi.getMode() != WIFI_AP_STA) || WiFi.status() != WL_CONNECTED){
    setup_wifi();
    if((WiFi.getMode() != WIFI_STA && WiFi.getMode() != WIFI_AP_STA) || WiFi.status() != WL_CONNECTED){
      res_json_print("(connect_ws) Cant connect WebSocket. WiFi not Ready.");
      return;
    }
  }
  if (ws_client.available()){
    res_json_print("(connect_ws) Disconnect WebSocket.");
    ws_client.close();
  }
  unsigned long timeout_ws = millis() + timeout_wifi_ws_connect_s * 1000;
  unsigned long next_try_ws = 0;
  bool first_switch_button = digitalRead(wifi_switch_button) == HIGH;
  res_json_print("(connect_ws) Connect WS to "+wl_config.ws_url);
  while((WiFi.getMode() == WIFI_STA || WiFi.getMode() == WIFI_AP_STA) && !ws_client.available() && 
  ((millis() < timeout_ws && ((first_switch_button == true && digitalRead(wifi_switch_button) == HIGH) || first_switch_button == false)) || (first_switch_button == false && digitalRead(wifi_switch_button) == LOW)
  )){
    blink_led(400);
    res_json_print("(connect_ws) Try Connect WS");
    if(millis()>=next_try_ws){
      ws_client.connect(wl_config.ws_url);
      next_try_ws = millis()+try_wifi_ws_connect_s*1000;
    }
    if(first_switch_button==false&&digitalRead(wifi_switch_button) == HIGH)
      first_switch_button==true;
  }
  if((WiFi.getMode() == WIFI_STA || WiFi.getMode() == WIFI_AP_STA) && !ws_client.available()){
    res_json_print("(connect_ws) Fail connect WebSocket");
    setup_ap();
    while(first_switch_button==true&&digitalRead(wifi_switch_button) == LOW);
  }else if (WiFi.getMode() == WIFI_AP || WiFi.getMode() == WIFI_AP_STA){
    res_json_print("(connect_ws) WiFi AP on.");
    digitalWrite(wifi_indicator_led,HIGH);
  }else if (ws_client.available())
    res_json_print("(connect_ws) WebSocket Connected.");
}
void loop_switch_wl() {
  if(digitalRead(wifi_switch_button) == LOW){
    if(target_timeout_switch_wifi == 0) {
      res_json_print("(loop_switch_wl) Hold "+String(timeout_switch_wifi_button_s)+" to switch");
      target_timeout_switch_wifi = millis() + timeout_switch_wifi_button_s * 1000;
    } else if (millis() >= target_timeout_switch_wifi){
      res_json_print("(loop_switch_wl) Switching...");
      if(WiFi.getMode() == WIFI_AP || WiFi.getMode() == WIFI_AP_STA) setup_wifi();
      else setup_ap();
      target_timeout_switch_wifi = 0;
      res_json_print("(loop_switch_wl) Please release button");
      while(digitalRead(wifi_switch_button) == LOW);
      res_json_print("(loop_switch_wl) Switch done");
    }
  } else if (target_timeout_switch_wifi > 0){
    res_json_print("(loop_switch_wl) Switch aborted");
    target_timeout_switch_wifi = 0;
  }
}

void blink_led(unsigned long dly_ms, bool toggle){
    digitalWrite(wifi_indicator_led,!toggle);
    delay(dly_ms);
    digitalWrite(wifi_indicator_led,toggle);
    delay(dly_ms);  
}
void blink_led(unsigned long dly_ms) {blink_led(dly_ms,false);}

void set_clk(unsigned long clk){
  long clk_diff = get_clk() - data_clk.t_s;
  data_clk.last_millis = millis();
  data_clk.t_s = clk;
  res_json_print("(set_clk) calibrate clk "+String(clk_diff));
}
unsigned long get_clk(){
  unsigned long last_millis = millis(); 
  if(last_millis >= 1000 && data_clk.last_millis <= (last_millis-1000)){
    data_clk.t_s += ((last_millis - data_clk.last_millis)/1000);
    data_clk.last_millis = last_millis;
  }else if(last_millis < data_clk.last_millis)
    data_clk.last_millis = last_millis;
  return data_clk.t_s;
}

void clear_ale_rdy(){
  digitalWrite(ale,LOW);
  digitalWrite(rdy,LOW);
  delayMicroseconds(t_pd_us);
}

bool send_a_i_d (uint8_t val){
  if(val<16){
    for(uint8_t a=0;a<4;a++) digitalWrite(ai0_a3_d_write[a],bitRead(val,a));
  }
  delayMicroseconds(t_pd_us);
  return val < 16;
}

bool write_address (uint8_t val){
  clear_ale_rdy();
  bool sended = send_a_i_d(val);
  if(sended){
    digitalWrite(ale,HIGH);
    delayMicroseconds(t_pd_us);
    digitalWrite(ale,LOW);
    delayMicroseconds(t_pd_us);
    send_a_i_d(0);
  }
  return sended;
}

bool write_iset_d (
  uint8_t val, bool d, 
  unsigned long before_us, 
  unsigned long toggle_us,
  unsigned long after_us
){
  clear_ale_rdy();
  if(val < 8){
    send_a_i_d(val);
    digitalWrite(ai0_a3_d_write[3],d?HIGH:LOW);
    if(before_us > 0) delayMicroseconds(before_us);
    digitalWrite(rdy,HIGH);
    delayMicroseconds(t_pd_us);
    if(toggle_us > 0) {
      delayMicroseconds(toggle_us);
      digitalWrite(ai0_a3_d_write[3],d?LOW:HIGH);
      delayMicroseconds(t_pd_us);
    }
    digitalWrite(rdy,LOW);
    delayMicroseconds(t_pd_us);
    if(after_us > 0) delayMicroseconds(after_us);
    send_a_i_d(0);
  } else send_a_i_d(0);
  return val < 8;
}

bool read_d_iset (uint8_t val){
  clear_ale_rdy();
  if(val < 8){
    send_a_i_d(val);
    digitalWrite(rdy,HIGH);
    uint8_t i_try=0;
    uint8_t i_true = 0;
    uint8_t i_false = 0;
    bool read_tmp[3] = {false, false, false};
    bool done_read=max_try_read==0;
    for(; !done_read && i_try<=max_try_read; i_try++){
      done_read = true;
      for(uint8_t i_test=0;i_test<3;i_test++){
        read_tmp[i_test] = false;
        delayMicroseconds(i_try*t_pd_us);
        read_tmp[i_test] = digitalRead(d_read) == HIGH;
        if(read_tmp[i_test]) i_true++; else i_false++;
        if(i_test>0 && read_tmp[i_test]!=read_tmp[i_test-1])
          done_read = false; 
      }
    }
    if(i_true>0 && i_false>0)
      res_json_print("(read_d_iset) UNKNOWN READ true "+String(i_true)+" false "+String(i_false));
    digitalWrite(rdy,LOW);
    delayMicroseconds(t_pd_us);
    send_a_i_d(0);
    return i_true >= i_false;
  } else {
    send_a_i_d(0);
    return false;
  }  
}

uint16_t read_adc_iset (uint8_t val) {
  clear_ale_rdy();
  if(val >= 8) return 0;
  send_a_i_d(val);
  digitalWrite(rdy,HIGH);
  delayMicroseconds(t_pd_us);
  delay(t_pd_readadc_ms);
  uint16_t tmp = analogRead(adc_read);
  digitalWrite(rdy,LOW);
  delayMicroseconds(t_pd_us);
  send_a_i_d(0);
  return tmp;
}

bool set_io (uint8_t io){
  if(io<16){
    write_address(io);
    write_iset(1);
    return true;
  }else{
    clear_ale_rdy();
    send_a_i_d(0);
    return false;
  }  
}
bool set_io_line (uint8_t io, uint8_t line){
  if(io<16 && line<8){
    set_io(io);
    write_address(line);
    write_iset(2);
    return true;
  }else{
    clear_ale_rdy();
    send_a_i_d(0);
    return false;
  }
}

bool write_io_line (
  uint8_t io, uint8_t line, bool d,
  unsigned long before_us, 
  unsigned long toggle_us,
  unsigned long after_us
){
    if(!set_io_line(io,line)) return false;
    write_iset_d(3,d,before_us,toggle_us,after_us);
    uint8_t i_try = 0;
    bool d_after = toggle_us>0?!d:d;
    data_io[io].line_state[line]=d_after;
    if(can_read_state){
      data_io[io].line_broken[line]=false;
      bool r = read_io_line(io,line);
      while(d_after != r && max_try_write > 0 && i_try <= max_try_write){
        write_iset_d(3,d_after,0,0,0);
        r = read_io_line(io,line);
        i_try++;
      }
      if(d_after!=r){
        data_io[io].line_broken[line]=true;
        res_json_print("(write_io_line) WRONG SET IO "+String(io<10?"0":"")+String(io)+" L "+String(line));
      }
    }
    return true;
}
bool read_io_line (uint8_t io, uint8_t line){
  if(io>=16 && line>=8) return false;
  if(can_read_state){
    if(!set_io_line(io,line)) return false;
    data_io[io].line_state[line] = read_d_iset(4);
    return data_io[io].line_state[line];
  }else
    return data_io[io].line_broken[line]?false:data_io[io].line_state[line];
}
uint16_t read_adc_io (uint8_t io) {
  if(!set_io_line(io,0)) return 0;
  data_io[io].adc_val=read_adc_iset(5);
  return data_io[io].adc_val;
}
bool reset_io(uint8_t io){
  if(!set_io_line(io,0)) return false;
  write_iset(6);
  return true;
}
void reset_cs(){
  write_iset(7);
}

bool check_json_req(String req){
  JsonVariant req_var = req_json["req"];
  if(req_var.isNull()){
    res_json_print("(check_json_req) req key is not found or empty");
    return false;
  }
  if(!req_var.is<String>()){
    res_json_print("(check_json_req) req key is not String");
    return false;
  }
  return req_var.as<String>().equalsIgnoreCase(req);
}
bool check_json_internal_id(JsonVariant i_id){
  if(i_id.isNull()){
    res_json_print("(check_json_internal_id) internal id is not found or empty");
    return false;
  }
  if(!i_id.is<int>() || i_id.as<int>()!=0){
    res_json_print("(check_json_internal_id) not supported");
    return false;
  }
  return true;  
}
bool check_json_io (JsonVariant io){
  if(io.isNull()){
    res_json_print("(check_json_io) io not found or empty");
    return false;
  }
  if(!io.is<unsigned int>()){
    res_json_print("(check_json_io) io not a number or positive number");
    return false;
  }
  if((io.as<unsigned int>())>=16){
    res_json_print("(check_json_io) io out of range");
    return false;
  }
  return true;
}
bool check_json_l (JsonVariant line){
  if(line.isNull()){
    res_json_print("(check_json_line) line not found or empty");
    return false;
  }
  if(!line.is<unsigned int>()){
    res_json_print("(check_json_line) line not a number or positive number");
    return false;
  }
  if((line.as<unsigned int>())>=8){
    res_json_print("(check_json_line) line out of range");
    return false;
  }
  return true;
}

bool req_json_get_state(bool from_ws) {
  if(!check_json_internal_id(req_json["internal_id"]))
    return false;
  res_json_set_state(from_ws);
  return true;
}
bool req_json_set_states() {
  JsonVariant steps_var = req_json["steps"];
  if(steps_var.isNull()){
    res_json_print("(req_json_set_states) steps not found or empty");
    return false;
  }
  if(!steps_var.is<JsonArray>()){
    res_json_print("(req_json_set_states) steps not an array");
    return false;
  }
  for(JsonVariant step_var : steps_var.as<JsonArray>()){
    if(!step_var.is<JsonObject>()){
      res_json_print("(req_json_set_states) some step not an object");
      continue;
    }
    JsonVariant st_var = step_var["st"];
    if(!check_json_internal_id(step_var["i_id"]) || !check_json_io(step_var["io"]) || !check_json_io(step_var["l"]))
      continue;
    if(!st_var.is<bool>()){
      res_json_print("(req_json_set_states) unknown state step");
      continue;
    }
    write_io_line (
      step_var["io"].as<unsigned int>(), step_var["l"].as<unsigned int>(), st_var.as<bool>(),
      step_var["b_us"].is<unsigned long>()?step_var["b_us"].as<unsigned long>():0, 
      0,
      step_var["a_us"].is<unsigned long>()?step_var["a_us"].as<unsigned long>():0
    );
  }
  res_json_set_state(false);
  return true;
}
bool res_json_reply_sync_id(bool to_ws){
  if(req_json["sync_id"].isNull())
    return false;
  DynamicJsonDocument res_json(64);
  res_json["reply_sync_id"]=req_json["sync_id"];
  res_json["clb_t_s"]=get_clk();
  if(to_ws){
    String str_json;
    serializeJson(res_json, str_json);
    if(ws_client.available())
      ws_client.send(str_json);
  }else {
    serializeJson(res_json, Serial);
    Serial.println();
  }
  return true;
}
void res_json_set_state(bool to_ws){
  DynamicJsonDocument res_json(3072);
  res_json["req"]="set_state";
  res_json["internal_id"]=0;
  res_json["clb_t_s"]=get_clk();
  JsonArray res_io = res_json.createNestedArray("io_l_state");
  for(uint8_t io=0;io<16;io++){
    JsonArray res_l = res_io.createNestedArray();
    for(uint8_t line=0;line<8;line++){
      if(data_io[io].line_broken[line]) res_l.add(nullptr);
      else res_l.add(read_io_line(io, line));
    }
  }
  if(to_ws){
    String str_json;
    serializeJson(res_json, str_json);
    if(ws_client.available())
      ws_client.send(str_json);
  }
  serializeJson(res_json, Serial);
  Serial.println();
}
bool req_json_get_adc(bool from_ws) {
  if(!check_json_internal_id(req_json["internal_id"]))
    return false;
  res_json_set_adc(from_ws);
  return true;
}
bool req_json_set_adc(bool from_ws){
  JsonVariant io_adc_var = req_json["io_adc"];
  if(!check_json_internal_id(req_json["internal_id"]))
    return false;
  if(io_adc_var.isNull()){
    res_json_print("(req_json_set_adc) io_adc not found or empty");
    return false;
  }
  if(!io_adc_var.is<JsonArray>()){
    res_json_print("(req_json_set_adc) io_adc not an array");
    return false;
  }
  uint8_t io = 0;
  for(JsonVariant adc_var : io_adc_var.as<JsonArray>()){
    data_io[io].adc_val = 0;
    data_io[io].last_adc_sent = 0;
    data_io[io].last_ms_sent = 0;
    if(adc_var.is<JsonObject>()){
      data_io[io].can_read = adc_var["e"].is<bool>()?adc_var["e"].as<bool>():false;
      data_io[io].margin_adc = adc_var["m_adc"].is<unsigned int>()?adc_var["m_adc"].as<unsigned int>():4095;
      data_io[io].margin_t_ms = adc_var["m_t_ms"].is<unsigned long>()?adc_var["m_t_ms"].as<unsigned long>():0;
    }else{
      res_json_print("(req_json_set_adc) some ADC configuration not an object");
      data_io[io].can_read = false;
      data_io[io].margin_adc = 4095;
      data_io[io].margin_t_ms = 0;      
    }
    io++;
  }
  for(;io<16;io++){
    data_io[io].adc_val = 0;
    data_io[io].last_adc_sent = 0;
    data_io[io].last_ms_sent = 0;
    data_io[io].can_read = false;
    data_io[io].margin_adc = 4095;
    data_io[io].margin_t_ms = 0;   
  }
  res_json_set_adc(from_ws);
  return true;
}
bool req_json_read_adc(bool from_ws){
  if(!check_json_internal_id(req_json["internal_id"]))
    return false;
  if(check_json_io(req_json["io"]))
    read_adc_io(req_json["io"].as<unsigned int>());
  res_json_set_adc(from_ws);
  return true;
}
void res_json_set_adc(bool to_ws){
  DynamicJsonDocument res_json(1536);
  res_json["req"]="set_adc";
  res_json["clb_t_s"]=get_clk();
  res_json["internal_id"]=0;
  JsonArray res_io = res_json.createNestedArray("io_adc");
  for(uint8_t io=0;io<16;io++){
    if(data_io[io].can_read)
      data_io[io].adc_val = read_adc_io(io);
    data_io[io].last_adc_sent = data_io[io].adc_val;
    data_io[io].last_ms_sent = millis();
    JsonObject res_adc = res_io.createNestedObject();
    res_adc["e"]=data_io[io].can_read;
    res_adc["m_adc"]=data_io[io].margin_adc;
    res_adc["m_t_ms"]=data_io[io].margin_t_ms;
    res_adc["val"]=data_io[io].last_adc_sent;
  }
  if(to_ws){
    String str_json;
    serializeJson(res_json, str_json);
    if(ws_client.available())
      ws_client.send(str_json);
  }
  serializeJson(res_json, Serial);
  Serial.println(); 
}
bool req_json_rst_io(){
  if(!check_json_internal_id(req_json["internal_id"]) || !check_json_io(req_json["io"]))
    return false;
  uint8_t io = req_json["io"].as<unsigned int>();
  reset_io(io);
  for(uint8_t line=0;line<8;line++){
    if(!can_read_state) data_io[io].line_state[line]=false; 
    data_io[io].line_broken[line]=false;
  }
  if(!can_read_state){
    setup_broken_io_cant_read_state();
  }
  res_json_set_state(false);
  return true;
}
bool req_json_rst_cs(){
  if(!check_json_internal_id(req_json["internal_id"]))
    return false;
  req_json_rst_all();
  return true;
}
void req_json_rst_all(){
  reset_cs();
  for(uint8_t io=0;io<16;io++){
    for(uint8_t line=0;line<8;line++){
      if(!can_read_state) data_io[io].line_state[line]=false; 
      data_io[io].line_broken[line]=false;
    }
  }
  if(!can_read_state)
    setup_broken_io_cant_read_state();
  res_json_set_state(false);
}
void req_json_duplicate_block(){
  res_json_print("(req_json_duplicate_block) Please change WebSocket URL");
  setup_ap();
}
bool req_json_ws_ping(){
  if(ws_client.available()){
    last_req_json_ws_ping=millis();
    if(last_req_json_ws_ping==0) last_req_json_ws_ping=1;
    ws_client.ping();
    while(loop_ping());
    return true;
  }else{
    res_json_ws_offline();
    return false;
  }
}
bool res_json_ws_pong(){
  unsigned long pong_millis=millis();
  if(last_req_json_ws_ping>0){
    DynamicJsonDocument res_json(600);
    res_json["req"] = "ws_pong";
    res_json["rtt_ms"] = last_req_json_ws_ping>pong_millis ? pong_millis : pong_millis-last_req_json_ws_ping;
    res_json["clb_t_s"] = get_clk();
    serializeJson(res_json,Serial);
    Serial.println();
    last_req_json_ws_ping=0;
    return true;
  }else{
    res_json_print("(res_json_ws_pong) Unknown Pong");
    return false;
  }
}
void res_json_ws_offline(){
  DynamicJsonDocument res_json(500);
  res_json["req"] = "ws_offline";
  res_json["clb_t_s"] = get_clk();
  serializeJson(res_json, Serial);
  Serial.println();
}
void res_json_err_req(bool to_ws){
  String full_json = "{\"req\":\"err_req\",\"clb_t_s\":"+String(get_clk())+",\"val\":";
  Serial.print(full_json);
  serializeJson(req_json,Serial);
  Serial.print("}");
  Serial.println();
  if(to_ws){
    String str_req;
    serializeJson(req_json,str_req);
    full_json += str_req;
    full_json += "}";
    if(ws_client.available())
      ws_client.send(full_json);
  }
}
void res_json_err_json(String msg_no_json){
  String str_json;
  DynamicJsonDocument res_json(1024);
  res_json["req"] = "err_json";
  res_json["val"] = msg_no_json;
  res_json["clb_t_s"] = get_clk();
  serializeJson(res_json, str_json);
  Serial.println();
  if(ws_client.available())
    ws_client.send(str_json);
}
void res_json_print(String str_print){
  DynamicJsonDocument res_json(400);
  res_json["req"] = "print";
  res_json["print"] = str_print;
  res_json["clb_t_s"] = get_clk();
  serializeJson(res_json, Serial);
  Serial.println();
}
void route_req_json(bool from_ws){
  JsonVariant req_var = req_json["req"];
  JsonVariant clk_var = req_json["srv_t_s"];
  if(!clk_var.isNull() && clk_var.is<unsigned long>())
    set_clk(clk_var.as<unsigned long>());
  if(req_var.isNull()){
    res_json_print("(route_req_json) req is not found or empty");
    res_json_err_req(from_ws);
  }else if(!req_var.is<String>()){
    res_json_print("(route_req_json) req is not a String");
    res_json_err_req(from_ws);    
  }else if(check_json_req("get_state"))
    req_json_get_state(true);
  else if(check_json_req("set_states"))
    req_json_set_states();
  else if(check_json_req("get_adc"))
    req_json_get_adc(true);
  else if(check_json_req("set_adc"))
    req_json_set_adc(true);
  else if(check_json_req("read_adc"))
    req_json_read_adc(from_ws);
  else if(check_json_req("rst_io"))
    req_json_rst_io();
  else if(check_json_req("rst_cs"))
    req_json_rst_cs();
  else if(check_json_req("rst_all"))
    req_json_rst_all();
  else if(check_json_req("duplicate_block"))
    req_json_duplicate_block();
  else if(check_json_req("ws_ping"))
    req_json_ws_ping();
  else{
    res_json_print("(route_req_json) unknown req");
    res_json_err_req(from_ws);    
  }
  res_json_reply_sync_id(from_ws);
  req_json.clear();
}
void loop_serial_json(){
  if(Serial.available()>0){
    DeserializationError err_json = deserializeJson(req_json,Serial);
    if(err_json) res_json_print("(loop_serial_json) cant parse json : "+String(err_json.c_str()));
    else route_req_json(false);
    blink_led(150,WiFi.getMode()==WIFI_AP);
    blink_led(150,WiFi.getMode()==WIFI_AP);
  }
}
void loop_update_json(){
  if(data_io[io_loop_update_json].can_read){
    read_adc_io(io_loop_update_json);
    uint16_t diff_adc = abs(data_io[io_loop_update_json].adc_val-data_io[io_loop_update_json].last_adc_sent);
    if(diff_adc>=data_io[io_loop_update_json].margin_adc || (diff_adc>0 && 
      (millis()-data_io[io_loop_update_json].last_ms_sent)>=data_io[io_loop_update_json].margin_t_ms
    )){
      res_json_print("(loop_update_json) there is adc changed");
      res_json_set_adc(true);
      blink_led(200,WiFi.getMode()==WIFI_AP);
    }
  }
  bool new_broken_line=false;
  for(uint8_t line=0;line<8;line++){
    if(data_io[io_loop_update_json].line_broken[line]) continue;
    bool old_state = data_io[io_loop_update_json].line_state[line];
    read_io_line(io_loop_update_json,line);
    if(data_io[io_loop_update_json].line_state[line] != old_state)
      write_io_line (
        io_loop_update_json, line, old_state,
        0,0,0
      );    
    new_broken_line = new_broken_line || data_io[io_loop_update_json].line_broken[line];
  }
  if(new_broken_line){
    res_json_print("(loop_update_json) there is state changed");
    res_json_set_state(true);
    blink_led(200,WiFi.getMode()==WIFI_AP);
  }
  io_loop_update_json++;
  if(io_loop_update_json>=16)
    io_loop_update_json=0;
}
