import zmq
import json
import signal
import sys
from datetime import datetime

# 브릿지 설정
EVENT_ADDR = "tcp://localhost:37359"

def listen_bridge():
    context = zmq.Context()
    
    # SUB 소켓 생성
    socket = context.socket(zmq.SUB)
    
    try:
        # 브릿지 이벤트 주소에 연결
        socket.connect(EVENT_ADDR)
        
        # 모든 토픽 구독 (""은 모든 메시지를 의미)
        # 특정 장치만 보고 싶다면 장치 ID를 입력: socket.subscribe("device_id")
        socket.subscribe("")
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Listening for bridge events on {EVENT_ADDR}...")
        print("Press Ctrl+C to stop.\n")
        
        while True:
            # 메시지 수신 (topic, payload 2개 프레임)
            try:
                topic, payload_raw = socket.recv_multipart()
                topic = topic.decode('utf-8')
                payload_str = payload_raw.decode('utf-8')
                
                # JSON 파싱 시도
                try:
                    payload = json.loads(payload_str)
                    formatted_payload = json.dumps(payload, indent=2)
                except json.JSONDecodeError:
                    formatted_payload = payload_str
                
                # 시간과 함께 출력
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print(f"[{timestamp}] TOPIC: {topic}")
                print(f"PAYLOAD: {formatted_payload}")
                print("-" * 50)
                
            except zmq.ZMQError as e:
                print(f"ZMQ Error: {e}")
                break

    except KeyboardInterrupt:
        print("\nStopping listener...")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        socket.close()
        context.term()

if __name__ == "__main__":
    # 실행 시 주소를 인자로 줄 수 있습니다. 예: python bridge_listen.py tcp://192.168.1.10:37359
    if len(sys.argv) > 1:
        EVENT_ADDR = sys.argv[1]
        
    listen_bridge()
