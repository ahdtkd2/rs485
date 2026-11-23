import paho.mqtt.client as mqtt
import re
from json import dumps as json_dumps
from functools import reduce
from collections import defaultdict
import json
import time
import threading

def log(msg):
    print(time.strftime('%Y-%m-%d %H:%M:%S'), msg, flush=True)

# ---- 애드온 구성(/data/options.json)에서 값 읽기 ----
# 코드에는 비밀번호를 적지 않음. HA 애드온 '구성' 탭에서 입력.
def _load_options():
    try:
        with open('/data/options.json', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}

_OPT  = _load_options()
_MQTT = _OPT.get('MQTT', {})
_ADV  = _OPT.get('Advanced', {})

MQTT_SERVER   = _MQTT.get('server', 'core-mosquitto')
MQTT_PORT     = int(_MQTT.get('port', 1883))
MQTT_USERNAME = _MQTT.get('username', '')
MQTT_PASSWORD = _MQTT.get('password', '')

class Device:
    def __init__(self, device_name, device_id, device_subid, device_class, child_device, mqtt_discovery, optional_info):
        self.device_name = device_name
        self.device_id = device_id
        self.device_subid = device_subid
        self.device_unique_id = 'rs485_' + self.device_id + '_' + self.device_subid
        self.device_class = device_class
        self.child_device = child_device
        self.mqtt_discovery = mqtt_discovery
        self.optional_info = optional_info

        self.__message_flag = {}            # {'power': '41'}
        self.__command_process_func = {}

        self.__status_messages_map = defaultdict(list)
        self.__command_messages_map = {}

    def register_status(self, message_flag, attr_name, regex, topic_class, device_name = None, process_func = lambda v: v):
        device_name = self.device_name if device_name == None else device_name
        self.__status_messages_map[message_flag].append({'regex': regex, 'process_func': process_func, 'device_name': device_name, 'attr_name': attr_name, 'topic_class': topic_class})

    def register_command(self, message_flag, attr_name, topic_class, process_func = lambda v: v):
        self.__command_messages_map[attr_name] = {'message_flag': message_flag, 'attr_name': attr_name, 'topic_class': topic_class, 'process_func': process_func}

    def parse_payload(self, payload_dict):
        result = {}
        device_family = [self] + self.child_device
        for device in device_family:
            for status in device.__status_messages_map[payload_dict['message_flag']]:
                topic = '/'.join([ROOT_TOPIC_NAME, device.device_class, device.device_name, status['attr_name']])
                result[topic] = status['process_func'](re.match(status['regex'], payload_dict['data'])[1])
        return result

    def get_command_payload_byte(self, attr_name, attr_value):  # command('power', 'ON')   command('percentage', 'middle')
        attr_value = self.__command_messages_map[attr_name]['process_func'](attr_value)

        command_payload = ['f7', self.device_id, self.device_subid, self.__command_messages_map[attr_name]['message_flag'], '01', attr_value]
        command_payload.append(Wallpad.xor(command_payload))
        command_payload.append(Wallpad.add(command_payload))
        return bytearray.fromhex(' '.join(command_payload))

    def get_command_flag(self, attr_name):
        return self.__command_messages_map[attr_name]['message_flag']

    def get_mqtt_discovery_payload(self):
        result = {
            '~': '/'.join([ROOT_TOPIC_NAME, self.device_class, self.device_name]),
            'name': self.device_name,
            'uniq_id': self.device_unique_id,
        }
        result.update(self.optional_info)
        for status_list in self.__status_messages_map.values():
            for status in status_list:
                result[status['topic_class']] = '/'.join(['~', status['attr_name']])

        for status_list in self.__command_messages_map.values():
            result[status_list['topic_class']] = '/'.join(['~', status_list['attr_name'], 'set'])

        result['device'] = {
            'identifiers': self.device_unique_id,
            'name': self.device_name
        }
        return json_dumps(result, ensure_ascii = False)

    def get_command_attr_list(self):
        return list(self.__command_messages_map.keys())

    def get_status_attr_list(self):
        return list(set([status['attr_name'] for status_list in self.__status_messages_map.values() for status in status_list]))

class Wallpad:
    _device_list = []

    def __init__(self):
        try:  # paho-mqtt 2.x
            self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
        except AttributeError:  # paho-mqtt 1.x
            self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect    = self.on_connect
        self.mqtt_client.on_message    = self.on_raw_message
        self.mqtt_client.on_disconnect = self.on_disconnect
        if MQTT_USERNAME:
            self.mqtt_client.username_pw_set(username=MQTT_USERNAME, password=MQTT_PASSWORD)
        self.mqtt_client.reconnect_delay_set(min_delay=1, max_delay=30)
        # 명령 재전송 대기열: {(device_id, subid, ack_flag): {...}}
        self._pending = {}
        self._pending_lock = threading.Lock()
        self._elev_last = 0       # 마지막 엘리베이터 호출 진행(43/44) 패킷 시각
        self._elev_on = False

    def listen(self):
        # 브로커가 아직 안 떠 있어도 죽지 않고 계속 재접속 시도
        log('MQTT 서버 %s:%s, 사용자 %s' % (MQTT_SERVER, MQTT_PORT, MQTT_USERNAME or '(익명)'))
        self.mqtt_client.connect_async(MQTT_SERVER, MQTT_PORT, keepalive=60)
        threading.Thread(target=self._retry_worker, daemon=True).start()
        self.mqtt_client.loop_forever(retry_first_connection=True)

    def on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            log('MQTT 접속 실패 rc=%s' % rc)
            return
        log('MQTT 접속 성공')
        client.publish(ELEVATOR_STATE_TOPIC, 'OFF', qos = 1, retain = False)
        # 재접속할 때마다 discovery/구독을 다시 등록
        self.register_mqtt_discovery()
        client.subscribe([(topic, 2) for topic in [ROOT_TOPIC_NAME + '/dev/raw'] + self.get_topic_list_to_listen()])

    # ---------- 명령 재전송 ----------
    def _send_command(self, device, attr_name, value):
        if device.device_id == '33' and attr_name == 'call':
            packet = bytearray.fromhex(ELEVATOR_CALL_PACKET)
            self.mqtt_client.publish(ROOT_TOPIC_NAME + '/dev/command', packet, qos = 2, retain = False)
            log('명령 전송: 엘리베이터 호출 [%s]' % packet.hex())
            now = time.time()
            with self._pending_lock:
                self._pending[('33', '01', '43')] = {'packet': packet, 'name': '엘리베이터', 'tries': 1, 'last': now, 'first': now}
            return
        # 풍량 0 은 기기가 무시함 -> 전원 OFF 로 변환
        if attr_name == 'percentage' and value.strip() in ('0', '0.0'):
            attr_name, value = 'power', 'OFF'
        packet = device.get_command_payload_byte(attr_name, value)
        flag = device.get_command_flag(attr_name)
        self.mqtt_client.publish(ROOT_TOPIC_NAME + '/dev/command', packet, qos = 2, retain = False)
        log('명령 전송: %s %s=%s [%s]' % (device.device_name, attr_name, value, packet.hex()))
        if flag in NO_RETRY_FLAGS:
            return
        # 응답 flag = 명령 flag + 0x80 (41->c1, 42->c2, 43->c3 ...)
        ack_flag = format(int(flag, 16) | 0x80, '02x')
        key = (device.device_id, device.device_subid, ack_flag)
        with self._pending_lock:
            now = time.time()
            self._pending[key] = {'packet': packet, 'name': device.device_name, 'tries': 1, 'last': now, 'first': now}

    def _check_ack(self, device_id, subid, flag):
        with self._pending_lock:
            if self._pending.pop((device_id, subid, flag), None) is not None:
                log('응답 확인: %s %s %s' % (device_id, subid, flag))
                return
            # 확인 응답(c1, c2..) 대신 상태 패킷(81)으로만 답하는 기기 처리
            if flag in STATUS_ACK_FLAGS.get(device_id, []):
                now = time.time()
                for key in [k for k in self._pending if k[0] == device_id and k[1] == subid]:
                    # 명령 직후 이미 오고 있던 상태 패킷은 무시 (0.2초 이후 것만 인정)
                    if now - self._pending[key]['first'] >= 0.2:
                        del self._pending[key]
                        log('상태 응답 확인: %s %s %s' % (device_id, subid, flag))

    def _retry_worker(self):
        while True:
            time.sleep(0.1)
            now = time.time()
            resend = []
            with self._pending_lock:
                for key, item in list(self._pending.items()):
                    if now - item['last'] < RETRY_INTERVAL:
                        continue
                    if item['tries'] >= MAX_RETRY:
                        log('응답 없음, 포기: %s' % item['name'])
                        del self._pending[key]
                        continue
                    item['tries'] += 1
                    item['last'] = now
                    resend.append(item)
            if self._elev_on and now - self._elev_last > ELEVATOR_TIMEOUT:
                self._elev_on = False
                self.mqtt_client.publish(ELEVATOR_STATE_TOPIC, 'OFF', qos = 1, retain = False)
                log('엘리베이터 호출 상태 자동 해제 (%d초 동안 진행 패킷 없음)' % ELEVATOR_TIMEOUT)
            for item in resend:
                log('재전송 %d회차: %s' % (item['tries'], item['name']))
                self.mqtt_client.publish(ROOT_TOPIC_NAME + '/dev/command', item['packet'], qos = 2, retain = False)

    def register_mqtt_discovery(self):
        # 예전 버전의 엘리베이터 '스위치' 엔티티 정리 (v1.2.0부터 버튼 + 상태 센서)
        self.mqtt_client.publish(HOMEASSISTANT_ROOT_TOPIC_NAME + '/switch/rs485_33_01/config', '', qos = 1, retain = True)
        for device in self._device_list:
            if device.mqtt_discovery:
                topic = '/'.join([HOMEASSISTANT_ROOT_TOPIC_NAME, device.device_class, device.device_unique_id, 'config'])
                payload = device.get_mqtt_discovery_payload()
                self.mqtt_client.publish(topic, payload, qos = 2, retain = True)

    def add_device(self, device_name, device_id, device_subid, device_class, child_device = [], mqtt_discovery = True, optional_info = {}):
        device = Device(device_name, device_id, device_subid, device_class, child_device, mqtt_discovery, optional_info)
        self._device_list.append(device)
        return device

    def get_device(self, **kwargs):
        if 'device_name' in kwargs:
            return [device for device in self._device_list if device.device_name == kwargs['device_name']][0]
        else:
            return [device for device in self._device_list if device.device_id == kwargs['device_id'] and device.device_subid == kwargs['device_subid']][0]

    def get_topic_list_to_listen(self):
        return ['/'.join([ROOT_TOPIC_NAME, device.device_class, device.device_name, attr_name, 'set']) for device in self._device_list for attr_name in set(device.get_status_attr_list() + device.get_command_attr_list())]

    @classmethod
    def xor(cls, hexstring_array):
        return format(reduce((lambda x, y: x^y), list(map(lambda x: int(x, 16), hexstring_array))), '02x')

    @classmethod
    def add(cls, hexstring_array): # hexstring_array ['f7', '32', ...]
        return format(reduce((lambda x, y: x+y), list(map(lambda x: int(x, 16), hexstring_array))), '02x')[-2:]

    @classmethod
    def is_valid(cls, payload_hexstring):
        payload_hexstring_array = [payload_hexstring[i:i+2] for i in range(0, len(payload_hexstring), 2)] # ['f7', '0e', '1f', '81', '04', '00', '00', '00', '00', '63', '0c']
        try:
            result = int(payload_hexstring_array[4], 16) + 7 == len(payload_hexstring_array) and cls.xor(payload_hexstring_array[:-2]) == payload_hexstring_array[-2:-1][0] and cls.add(payload_hexstring_array[:-1]) == payload_hexstring_array[-1:][0]
            return result
        except:
            return False

    def on_raw_message(self, client, userdata, msg):
        if msg.topic == ROOT_TOPIC_NAME + '/dev/raw': # ew11이 MQTT에 rs485 패킷을 publish하는 경우
            for payload_raw_bytes in msg.payload.split(b'\xf7')[1:]: # payload 내에 여러 메시지가 있는 경우, \f7 disappear as delimiter here
                payload_hexstring = 'f7' + payload_raw_bytes.hex() # 'f7361f810f000001000017179817981717969896de22'
                try:
                    if self.is_valid(payload_hexstring):
                        m = re.match(r'f7(?P<device_id>0e|12|32|33|36)(?P<device_subid>[0-9a-f]{2})(?P<message_flag>[0-9a-f]{2})(?:[0-9a-f]{2})(?P<data>[0-9a-f]*)(?P<xor>[0-9a-f]{2})(?P<add>[0-9a-f]{2})', payload_hexstring)
                        if m is None:      # 등록되지 않은 기기(예: 0x39 원격검침)는 무시
                            continue
                        payload_dict = m.groupdict()
                        if not any(d.device_id == payload_dict['device_id'] and d.device_subid == payload_dict['device_subid'] for d in self._device_list):
                            continue
                        self._check_ack(payload_dict['device_id'], payload_dict['device_subid'], payload_dict['message_flag'])
                        if payload_dict['device_id'] == '33' and payload_dict['message_flag'] in ('43', '44'):
                            self._elev_last = time.time()
                            self._elev_on = not (payload_dict['message_flag'] == '44' and payload_dict['data'][:2] == '05')

                        for topic, value in self.get_device(device_id = payload_dict['device_id'], device_subid = payload_dict['device_subid']).parse_payload(payload_dict).items():
                            client.publish(topic, value, qos = 1, retain = False)
                    else:
                        continue
                except Exception as e:
                    client.publish(ROOT_TOPIC_NAME + '/dev/error', payload_hexstring, qos = 0, retain = False)
                    log('패킷 해석 실패: %s (%s)' % (payload_hexstring, e))

        else: # homeassistant에서 명령하여 MQTT topic을 publish하는 경우
            try:
                topic_split = msg.topic.split('/') # rs485_2mqtt/light/침실등/power/set
                device = self.get_device(device_name = topic_split[2])
                self._send_command(device, topic_split[3], msg.payload.decode())
            except Exception as e:
                # 잘못된 명령 하나 때문에 애드온 전체가 죽지 않도록
                log('명령 처리 오류: %s %s (%s)' % (msg.topic, msg.payload, e))

    def on_disconnect(self, client, userdata, rc):
        # 예외를 던지지 않음 -> loop_forever가 자동 재접속
        log('MQTT 연결 끊김 rc=%s, 재접속 시도' % rc)

MAX_RETRY = int(_ADV.get('max_retry', 3)) + 1        # 최초 1회 + 재전송 횟수
RETRY_INTERVAL = float(_ADV.get('retry_interval', 1.2)) # 응답 없으면 재전송 간격(초)
NO_RETRY_FLAGS = []
# 현관 패널이 호출 버튼을 눌렀을 때 월패드 폴링에 답하는 패킷 (2026-11-23 캡처로 확인)
ELEVATOR_CALL_PACKET = 'f7330181030024006336'
ELEVATOR_STATE_TOPIC = 'rs485_2mqtt/binary_sensor/엘리베이터 호출 상태/state'
ELEVATOR_TIMEOUT = 10   # '오는 중'(44) 패킷은 약 2초마다 옴. 10초 동안 없으면 꺼짐 처리
STATUS_ACK_FLAGS = {}  # 전열교환기도 c1/c2 확인 응답을 보냄 (2026-11-23 캡처로 확인)

ROOT_TOPIC_NAME = 'rs485_2mqtt'
HOMEASSISTANT_ROOT_TOPIC_NAME = 'homeassistant'
wallpad = Wallpad()

packet_2_payload_percentage = {'00': '0', '01': '1', '02': '2', '03': '3'}
packet_2_payload_oscillation = {'03': 'oscillate_on', '00': 'oscillation_off', '01': 'oscillate_off'}

### 전열교환기 ###
optional_info = {'optimistic': 'false', 'speed_range_min': 1, 'speed_range_max': 3}
전열교환기 = wallpad.add_device(device_name = '전열교환기', device_id = '32', device_subid = '01', device_class = 'fan', optional_info = optional_info)
전열교환기.register_status(message_flag = '01', attr_name = 'availability', topic_class ='availability_topic',      regex = r'()', process_func = lambda v: 'online')
전열교환기.register_status(message_flag = '81', attr_name = 'power',        topic_class ='state_topic',             regex = r'00(0[01])0[0-3][0-9a-f]{2}00', process_func = lambda v: 'ON' if v == '01' else 'OFF')
전열교환기.register_status(message_flag = 'c1', attr_name = 'power',        topic_class ='state_topic',             regex = r'00(0[01])0[0-3][0-9a-f]{2}00', process_func = lambda v: 'ON' if v == '01' else 'OFF')
전열교환기.register_status(message_flag = '81', attr_name = 'percentage',   topic_class ='percentage_state_topic',  regex = r'000[01](0[0-3])[0-9a-f]{2}00', process_func = lambda v: packet_2_payload_percentage[v])
전열교환기.register_status(message_flag = 'c2', attr_name = 'percentage',   topic_class ='percentage_state_topic',  regex = r'000[01](0[0-3])[0-9a-f]{2}00', process_func = lambda v: packet_2_payload_percentage[v])
전열교환기.register_status(message_flag = '81', attr_name = 'heat',         topic_class ='oscillation_state_topic', regex = r'000[01]0[0-3]([0-9a-f]{2})00', process_func = lambda v: 'oscillate_on' if v == '03' else 'oscillate_off')
전열교환기.register_status(message_flag = 'c3', attr_name = 'heat',         topic_class ='oscillation_state_topic', regex = r'000[01]0[0-3]([0-9a-f]{2})00', process_func = lambda v: 'oscillate_on' if v == '03' else 'oscillate_off')

전열교환기.register_command(message_flag = '41', attr_name = 'power',       topic_class = 'command_topic', process_func = lambda v: '01' if v =='ON' else '00')
전열교환기.register_command(message_flag = '42', attr_name = 'percentage',  topic_class = 'percentage_command_topic', process_func = lambda v: {payload: packet for packet, payload in packet_2_payload_percentage.items()}[v])
전열교환기.register_command(message_flag = '43', attr_name = 'heat',        topic_class = 'oscillation_command_topic', process_func = lambda v: {payload: packet for packet, payload in packet_2_payload_oscillation.items()}[v])

### 가스차단기 ###
optional_info = {'optimistic': 'false'}
가스 = wallpad.add_device(device_name = '가스', device_id = '12', device_subid = '01', device_class = 'switch', optional_info = optional_info)
가스.register_status(message_flag = '01', attr_name = 'availability', topic_class ='availability_topic', regex = r'()', process_func = lambda v: 'online')
가스.register_status(message_flag = '81', attr_name = 'power', topic_class ='state_topic', regex = r'00(0[12])', process_func = lambda v: 'ON' if v == '01' else 'OFF')
가스.register_status(message_flag = 'c1', attr_name = 'power', topic_class ='state_topic', regex = r'00(0[12])', process_func = lambda v: 'ON' if v == '01' else 'OFF')

가스.register_command(message_flag = '41', attr_name = 'power', topic_class = 'command_topic', process_func = lambda v: '01' if v == 'ON' else '00')

### 조명 ###
optional_info = {'optimistic': 'false'}
거실등1    = wallpad.add_device(device_name = '거실등1', device_id = '0e', device_subid = '11', device_class = 'light', optional_info = optional_info)
거실등2    = wallpad.add_device(device_name = '거실등2', device_id = '0e', device_subid = '12', device_class = 'light', optional_info = optional_info)
복도등     = wallpad.add_device(device_name = '복도등',  device_id = '0e', device_subid = '13', device_class = 'light', optional_info = optional_info)
침실등     = wallpad.add_device(device_name = '침실등',  device_id = '0e', device_subid = '21', device_class = 'light', optional_info = optional_info)
침실등2    = wallpad.add_device(device_name = '침실등2',  device_id = '0e', device_subid = '31', device_class = 'light', optional_info = optional_info)
침실등3    = wallpad.add_device(device_name = '침실등3',  device_id = '0e', device_subid = '41', device_class = 'light', optional_info = optional_info)
알파룸등    = wallpad.add_device(device_name = '알파룸등',  device_id = '0e', device_subid = '51', device_class = 'light', optional_info = optional_info)
#알파룸 = wallpad.add_device(device_name='알파룸', device_id='0e', device_subid='51', child_device = ["알파룸등"], device_class='light', optional_info=optional_info)
주방등1    = wallpad.add_device(device_name = '주방등1',  device_id = '0e', device_subid = '71', device_class = 'light', optional_info = optional_info)
주방등2    = wallpad.add_device(device_name = '주방등2',  device_id = '0e', device_subid = '72', device_class = 'light', optional_info = optional_info)
거실등전체 = wallpad.add_device(device_name = '거실등 전체', device_id = '0e', device_subid = '1f', device_class = 'light', mqtt_discovery = False, child_device = [거실등1, 거실등2, 복도등])
침실등전체 = wallpad.add_device(device_name = '알파룸등 전체', device_id = '0e', device_subid = '2f', device_class = 'light', mqtt_discovery = False, child_device = [침실등])
침실등2전체 = wallpad.add_device(device_name = '알파룸등 전체', device_id = '0e', device_subid = '3f', device_class = 'light', mqtt_discovery = False, child_device = [침실등2])
침실등3전체 = wallpad.add_device(device_name = '알파룸등 전체', device_id = '0e', device_subid = '4f', device_class = 'light', mqtt_discovery = False, child_device = [침실등3])
알파룸등전체 = wallpad.add_device(device_name = '알파룸등 전체', device_id = '0e', device_subid = '5f', device_class = 'light', mqtt_discovery = False, child_device = [알파룸등])
주방등전체 = wallpad.add_device(device_name = '주방등 전체', device_id = '0e', device_subid = '7f', device_class = 'light', mqtt_discovery = False, child_device = [주방등1,주방등2])

거실등전체.register_status(message_flag = '01', attr_name = 'availability', topic_class ='availability_topic', regex = r'()', process_func = lambda v: 'online')
주방등전체.register_status(message_flag = '01', attr_name = 'availability', topic_class ='availability_topic', regex = r'()', process_func = lambda v: 'online')
침실등전체.register_status(message_flag = '01', attr_name = 'availability', topic_class ='availability_topic', regex = r'()', process_func = lambda v: 'online')
침실등2전체.register_status(message_flag = '01', attr_name = 'availability', topic_class ='availability_topic', regex = r'()', process_func = lambda v: 'online')
침실등3전체.register_status(message_flag = '01', attr_name = 'availability', topic_class ='availability_topic', regex = r'()', process_func = lambda v: 'online')
알파룸등전체.register_status(message_flag = '01', attr_name = 'availability', topic_class ='availability_topic', regex = r'()', process_func = lambda v: 'online')


#알파룸.register_status(message_flag='81', attr_name='power', topic_class='state_topic', regex=r'0[01](0[01])', process_func=lambda v: 'ON' if v == '01' else 'OFF')
#알파룸.register_status(message_flag='c1', attr_name='power', topic_class='state_topic', regex=r'0[01](0[01])', process_func=lambda v: 'ON' if v == '01' else 'OFF')
#알파룸.register_command(message_flag='41', attr_name='power', topic_class='command_topic', process_func=lambda v: '01' if v == 'ON' else '00')

거실등1.register_status(message_flag = '81', attr_name = 'power', topic_class ='state_topic', regex = r'00(0[01])0[01]0[01]', process_func = lambda v: 'ON' if v == '01' else 'OFF')
거실등2.register_status(message_flag = '81', attr_name = 'power', topic_class ='state_topic', regex = r'000[01](0[01])0[01]', process_func = lambda v: 'ON' if v == '01' else 'OFF')
복도등.register_status( message_flag = '81', attr_name = 'power', topic_class ='state_topic', regex = r'000[01]0[01](0[01])', process_func = lambda v: 'ON' if v == '01' else 'OFF')
침실등.register_status( message_flag = '81', attr_name = 'power', topic_class ='state_topic', regex = r'00(0[01])', process_func = lambda v: 'ON' if v == '01' else 'OFF')
침실등2.register_status( message_flag = '81', attr_name = 'power', topic_class ='state_topic', regex = r'00(0[01])', process_func = lambda v: 'ON' if v == '01' else 'OFF')
침실등3.register_status( message_flag = '81', attr_name = 'power', topic_class ='state_topic', regex = r'00(0[01])', process_func = lambda v: 'ON' if v == '01' else 'OFF')
알파룸등.register_status( message_flag = '81', attr_name = 'power', topic_class ='state_topic', regex = r'00(0[01])', process_func = lambda v: 'ON' if v == '01' else 'OFF')
주방등1.register_status( message_flag = '81', attr_name = 'power', topic_class ='state_topic', regex =  r'00(0[01])0[01]', process_func = lambda v: 'ON' if v == '01' else 'OFF')
주방등2.register_status( message_flag = '81', attr_name = 'power', topic_class ='state_topic', regex =  r'000[01](0[01])', process_func = lambda v: 'ON' if v == '01' else 'OFF')

거실등1.register_status(message_flag = 'c1', attr_name = 'power', topic_class ='state_topic', regex = r'00(0[01])', process_func = lambda v: 'ON' if v == '01' else 'OFF')
거실등2.register_status(message_flag = 'c1', attr_name = 'power', topic_class ='state_topic', regex = r'00(0[01])', process_func = lambda v: 'ON' if v == '01' else 'OFF')
복도등.register_status( message_flag = 'c1', attr_name = 'power', topic_class ='state_topic', regex = r'00(0[01])', process_func = lambda v: 'ON' if v == '01' else 'OFF')
침실등.register_status( message_flag = 'c1', attr_name = 'power', topic_class ='state_topic', regex = r'00(0[01])', process_func = lambda v: 'ON' if v == '01' else 'OFF')
침실등2.register_status( message_flag = 'c1', attr_name = 'power', topic_class ='state_topic', regex = r'00(0[01])', process_func = lambda v: 'ON' if v == '01' else 'OFF')
침실등3.register_status( message_flag = 'c1', attr_name = 'power', topic_class ='state_topic', regex = r'00(0[01])', process_func = lambda v: 'ON' if v == '01' else 'OFF')
알파룸등.register_status( message_flag = 'c1', attr_name = 'power', topic_class ='state_topic', regex = r'00(0[01])', process_func = lambda v: 'ON' if v == '01' else 'OFF')
주방등1.register_status( message_flag = 'c1', attr_name = 'power', topic_class ='state_topic', regex = r'00(0[01])', process_func = lambda v: 'ON' if v == '01' else 'OFF')
주방등2.register_status( message_flag = 'c1', attr_name = 'power', topic_class ='state_topic', regex = r'00(0[01])', process_func = lambda v: 'ON' if v == '01' else 'OFF')

거실등1.register_command(message_flag = '41', attr_name = 'power', topic_class = 'command_topic', process_func = lambda v: '01' if v =='ON' else '00') # 'ON': '01' / 'OFF': '00'
거실등2.register_command(message_flag = '41', attr_name = 'power', topic_class = 'command_topic', process_func = lambda v: '01' if v =='ON' else '00')
복도등.register_command( message_flag = '41', attr_name = 'power', topic_class = 'command_topic', process_func = lambda v: '01' if v =='ON' else '00')
침실등.register_command( message_flag = '41', attr_name = 'power', topic_class = 'command_topic', process_func = lambda v: '01' if v =='ON' else '00')
침실등2.register_command( message_flag = '41', attr_name = 'power', topic_class = 'command_topic', process_func = lambda v: '01' if v =='ON' else '00')
침실등3.register_command( message_flag = '41', attr_name = 'power', topic_class = 'command_topic', process_func = lambda v: '01' if v =='ON' else '00')
알파룸등.register_command( message_flag = '41', attr_name = 'power', topic_class = 'command_topic', process_func = lambda v: '01' if v =='ON' else '00')
주방등1.register_command( message_flag = '41', attr_name = 'power', topic_class = 'command_topic', process_func = lambda v: '01' if v =='ON' else '00')
주방등2.register_command( message_flag = '41', attr_name = 'power', topic_class = 'command_topic', process_func = lambda v: '01' if v =='ON' else '00')

### 난방 ###
optional_info = {'modes': ['off', 'heat'], 'temp_step': 1, 'precision': 1.0, 'min_temp': 5.0, 'max_temp': 35.0, 'send_if_off': 'false'}
거실난방 =  wallpad.add_device(device_name = '거실 난방',   device_id = '36', device_subid = '11', device_class = 'climate', optional_info = optional_info)
침실난방 =  wallpad.add_device(device_name = '침실 난방',   device_id = '36', device_subid = '12', device_class = 'climate', optional_info = optional_info)
침실2난방 =  wallpad.add_device(device_name = '침실2 난방',   device_id = '36', device_subid = '13', device_class = 'climate', optional_info = optional_info)
침실3난방 =  wallpad.add_device(device_name = '침실3 난방',   device_id = '36', device_subid = '14', device_class = 'climate', optional_info = optional_info)
알파룸난방= wallpad.add_device(device_name = '알파룸 난방', device_id = '36', device_subid = '15', device_class = 'climate', optional_info = optional_info)
난방전체 =  wallpad.add_device(device_name = '난방 전체',   device_id = '36', device_subid = '1f', device_class = 'climate', mqtt_discovery = False, child_device = [거실난방, 침실난방, 침실2난방, 침실3난방, 알파룸난방])

난방전체.register_status(message_flag = '01', attr_name = 'availability', regex = r'()', topic_class ='availability_topic', process_func = lambda v: 'online')

난방전체.register_status(message_flag = '01', attr_name = 'availability', regex = r'()', topic_class ='availability_topic', process_func = lambda v: 'online')

for message_flag in ['81', 'c3', 'c4', 'c5']:
    거실난방.register_status(  message_flag = message_flag, attr_name = 'power', topic_class = 'mode_state_topic', regex = r'00([\da-fA-F]{2})[\da-fA-F]{2}[\da-fA-F]{4}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}', process_func = lambda v: 'heat' if format(int(v, 16), '05b')[4] == '1' else 'off')
    침실난방.register_status(  message_flag = message_flag, attr_name = 'power', topic_class = 'mode_state_topic', regex = r'00([\da-fA-F]{2})[\da-fA-F]{2}[\da-fA-F]{4}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}', process_func = lambda v: 'heat' if format(int(v, 16), '05b')[3] == '1' else 'off')
    침실2난방.register_status(  message_flag = message_flag, attr_name = 'power', topic_class = 'mode_state_topic', regex = r'00([\da-fA-F]{2})[\da-fA-F]{2}[\da-fA-F]{4}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}', process_func = lambda v: 'heat' if format(int(v, 16), '05b')[2] == '1' else 'off')
    침실3난방.register_status(  message_flag = message_flag, attr_name = 'power', topic_class = 'mode_state_topic', regex = r'00([\da-fA-F]{2})[\da-fA-F]{2}[\da-fA-F]{4}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}', process_func = lambda v: 'heat' if format(int(v, 16), '05b')[1] == '1' else 'off')
    알파룸난방.register_status(  message_flag = message_flag, attr_name = 'power', topic_class = 'mode_state_topic', regex = r'00([\da-fA-F]{2})[\da-fA-F]{2}[\da-fA-F]{4}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}', process_func = lambda v: 'heat' if format(int(v, 16), '05b')[0] == '1' else 'off')

    거실난방.register_status(  message_flag = message_flag, attr_name = 'away_mode', topic_class = 'away_mode_state_topic', regex = r'00([\da-fA-F]{2})[\da-fA-F]{2}[\da-fA-F]{4}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}', process_func = lambda v: 'ON' if format(int(v, 16), '05b')[4] == '1' else 'OFF')
    침실난방.register_status(  message_flag = message_flag, attr_name = 'away_mode', topic_class = 'away_mode_state_topic', regex = r'00([\da-fA-F]{2})[\da-fA-F]{2}[\da-fA-F]{4}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}', process_func = lambda v: 'ON' if format(int(v, 16), '05b')[3] == '1' else 'OFF')
    침실2난방.register_status(  message_flag = message_flag, attr_name = 'away_mode', topic_class = 'away_mode_state_topic', regex = r'00([\da-fA-F]{2})[\da-fA-F]{2}[\da-fA-F]{4}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}', process_func = lambda v: 'ON' if format(int(v, 16), '05b')[2] == '1' else 'OFF')
    침실3난방.register_status(  message_flag = message_flag, attr_name = 'away_mode', topic_class = 'away_mode_state_topic', regex = r'00([\da-fA-F]{2})[\da-fA-F]{2}[\da-fA-F]{4}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}', process_func = lambda v: 'ON' if format(int(v, 16), '05b')[1] == '1' else 'OFF')
    알파룸난방.register_status(  message_flag = message_flag, attr_name = 'away_mode', topic_class = 'away_mode_state_topic', regex = r'00([\da-fA-F]{2})[\da-fA-F]{2}[\da-fA-F]{4}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}', process_func = lambda v: 'ON' if format(int(v, 16), '05b')[0] == '1' else 'OFF')

    거실난방.register_status(  message_flag = message_flag, attr_name = 'targettemp',  topic_class ='temperature_state_topic',   regex = r'00[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{4}([\da-fA-F]{2})[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}', process_func = lambda v: int(v, 16) % 128 + int(v, 16) // 128 * 0.5)
    침실난방.register_status(  message_flag = message_flag, attr_name = 'targettemp',  topic_class ='temperature_state_topic',   regex = r'00[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{4}[\da-fA-F]{2}[\da-fA-F]{2}([\da-fA-F]{2})[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}', process_func = lambda v: int(v, 16) % 128 + int(v, 16) // 128 * 0.5)
    침실2난방.register_status(  message_flag = message_flag, attr_name = 'targettemp',  topic_class ='temperature_state_topic',   regex = r'00[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{4}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}([\da-fA-F]{2})[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}', process_func = lambda v: int(v, 16) % 128 + int(v, 16) // 128 * 0.5)
    침실3난방.register_status(  message_flag = message_flag, attr_name = 'targettemp',  topic_class ='temperature_state_topic',   regex = r'00[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{4}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}([\da-fA-F]{2})[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}', process_func = lambda v: int(v, 16) % 128 + int(v, 16) // 128 * 0.5)
    알파룸난방.register_status(  message_flag = message_flag, attr_name = 'targettemp',  topic_class ='temperature_state_topic',   regex = r'00[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{4}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}([\da-fA-F]{2})[\da-fA-F]{2}', process_func = lambda v: int(v, 16) % 128 + int(v, 16) // 128 * 0.5)
 
    거실난방.register_status(  message_flag = message_flag, attr_name = 'currenttemp', topic_class ='current_temperature_topic', regex = r'00[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{4}[\da-fA-F]{2}([\da-fA-F]{2})[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}', process_func = lambda v: int(v, 16) % 128 + int(v, 16) // 128 * 0.5)
    침실난방.register_status(  message_flag = message_flag, attr_name = 'currenttemp', topic_class ='current_temperature_topic', regex = r'00[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{4}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}([\da-fA-F]{2})[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}', process_func = lambda v: int(v, 16) % 128 + int(v, 16) // 128 * 0.5)
    침실2난방.register_status(  message_flag = message_flag, attr_name = 'currenttemp', topic_class ='current_temperature_topic', regex = r'00[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{4}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}([\da-fA-F]{2})[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}', process_func = lambda v: int(v, 16) % 128 + int(v, 16) // 128 * 0.5)
    침실3난방.register_status(  message_flag = message_flag, attr_name = 'currenttemp', topic_class ='current_temperature_topic', regex = r'00[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{4}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}([\da-fA-F]{2})[\da-fA-F]{2}[\da-fA-F]{2}', process_func = lambda v: int(v, 16) % 128 + int(v, 16) // 128 * 0.5)
    알파룸난방.register_status(  message_flag = message_flag, attr_name = 'currenttemp', topic_class ='current_temperature_topic', regex = r'00[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{4}[\da-fA-F]{2}([\da-fA-F]{2})[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}[\da-fA-F]{2}([\da-fA-F]{2})', process_func = lambda v: int(v, 16) % 128 + int(v, 16) // 128 * 0.5)



난방전체.register_command(message_flag = '43', attr_name = 'power', topic_class = 'mode_command_topic', process_func = lambda v: '01' if v == 'heat' else '00')

거실난방.register_command(message_flag = '43', attr_name = 'power', topic_class = 'mode_command_topic', process_func = lambda v: '01' if v == 'heat' else '00')
거실난방.register_command(message_flag = '44', attr_name = 'targettemp', topic_class = 'temperature_command_topic', process_func = lambda v: format(int(round(float(v))), '02x'))
거실난방.register_command(message_flag = '45', attr_name = 'away_mode', topic_class = 'away_mode_command_topic', process_func = lambda v: '01' if v =='ON' else '00')

침실난방.register_command(message_flag = '43', attr_name = 'power', topic_class = 'mode_command_topic', process_func = lambda v: '01' if v == 'heat' else '00')
침실난방.register_command(message_flag = '44', attr_name = 'targettemp', topic_class = 'temperature_command_topic', process_func = lambda v: format(int(round(float(v))), '02x'))
침실난방.register_command(message_flag = '45', attr_name = 'away_mode', topic_class = 'away_mode_command_topic', process_func = lambda v: '01' if v =='ON' else '00')

침실2난방.register_command(message_flag = '43', attr_name = 'power', topic_class = 'mode_command_topic', process_func = lambda v: '01' if v == 'heat' else '00')
침실2난방.register_command(message_flag = '44', attr_name = 'targettemp', topic_class = 'temperature_command_topic', process_func = lambda v: format(int(round(float(v))), '02x'))
침실2난방.register_command(message_flag = '45', attr_name = 'away_mode', topic_class = 'away_mode_command_topic', process_func = lambda v: '01' if v =='ON' else '00')

침실3난방.register_command(message_flag = '43', attr_name = 'power', topic_class = 'mode_command_topic', process_func = lambda v: '01' if v == 'heat' else '00')
침실3난방.register_command(message_flag = '44', attr_name = 'targettemp', topic_class = 'temperature_command_topic', process_func = lambda v: format(int(round(float(v))), '02x'))
침실3난방.register_command(message_flag = '45', attr_name = 'away_mode', topic_class = 'away_mode_command_topic', process_func = lambda v: '01' if v =='ON' else '00')

알파룸난방.register_command(message_flag = '43', attr_name = 'power', topic_class = 'mode_command_topic', process_func = lambda v: '01' if v == 'heat' else '00')
알파룸난방.register_command(message_flag = '44', attr_name = 'targettemp', topic_class = 'temperature_command_topic', process_func = lambda v: format(int(round(float(v))), '02x'))
알파룸난방.register_command(message_flag = '45', attr_name = 'away_mode', topic_class = 'away_mode_command_topic', process_func = lambda v: '01' if v =='ON' else '00')

### 엘리베이터 (현관 패널 0x33) ###
# 호출 흐름 (2026-11-23 캡처)
#   패널 -> 월패드 : f7 33 01 81 03 [00 24 00]   (평소 00 04 00, 0x20 = 호출 요청)
#   월패드 -> 패널 : f7 33 01 43 02 [10 00]      (호출 접수)
#   월패드 -> 패널 : f7 33 01 44 01 [01]         (호출 중, 약 2초마다)
#   월패드 -> 패널 : f7 33 01 44 01 [05]         (도착)
엘리베이터 = wallpad.add_device(device_name = '엘리베이터 호출', device_id = '33', device_subid = '01', device_class = 'button', optional_info = {'icon': 'mdi:elevator'})
엘리베이터상태 = wallpad.add_device(device_name = '엘리베이터 호출 상태', device_id = '33', device_subid = '01s', device_class = 'binary_sensor', optional_info = {'icon': 'mdi:elevator-passenger'})
엘리베이터.child_device.append(엘리베이터상태)
엘리베이터.register_status(message_flag = '01', attr_name = 'availability', topic_class = 'availability_topic', regex = r'()', process_func = lambda v: 'online')
엘리베이터상태.register_status(message_flag = '01', attr_name = 'availability', topic_class = 'availability_topic', regex = r'()', process_func = lambda v: 'online')
엘리베이터상태.register_status(message_flag = '43', attr_name = 'state', topic_class = 'state_topic', regex = r'(1)', process_func = lambda v: 'ON')
엘리베이터상태.register_status(message_flag = '44', attr_name = 'state', topic_class = 'state_topic', regex = r'([0-9a-f]{2})', process_func = lambda v: 'OFF' if v == '05' else 'ON')
엘리베이터.register_command(message_flag = '81', attr_name = 'call', topic_class = 'command_topic', process_func = lambda v: v)

wallpad.listen()
