# from flask import Flask, render_template, request, jsonify
# import pandas as pd
# from datetime import datetime, timedelta
# import math
# import os
#
# app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), 'templates'))
#
# print("正在加载数据...")
# df = pd.read_csv(
#     r'D:\Document\公交车\清洗后数据_全量合并.csv',
#     parse_dates=['GPSSHIJIAN_'],
#     encoding='gb18030'
# )
# df0507 = df[df['GPSSHIJIAN_'].dt.date == pd.Timestamp('2022-05-07').date()].copy()
# df0507['LAT'] = df0507['WEIDU'] / 600000
# df0507['LNG'] = df0507['JINGDU'] / 600000
# df0507 = df0507.sort_values('GPSSHIJIAN_')
# print(f"5月7日数据加载完成，共 {len(df0507)} 条记录")
#
# BAIDU_AK = "ROGTO1NWH9XMRePYdQcRPkmxFIyspC8z"
#
# # ====================== 本地坐标转换 ======================
# def wgs84_to_gcj02(lng, lat):
#     a = 6378245.0
#     ee = 0.00669342162296594323
#     def transform_lat(x, y):
#         ret = -100.0 + 2.0*x + 3.0*y + 0.2*y*y + 0.1*x*y + 0.2*math.sqrt(abs(x))
#         ret += (20.0*math.sin(6.0*x*math.pi) + 20.0*math.sin(2.0*x*math.pi)) * 2.0/3.0
#         ret += (20.0*math.sin(y*math.pi) + 40.0*math.sin(y/3.0*math.pi)) * 2.0/3.0
#         ret += (160.0*math.sin(y/12.0*math.pi) + 320.0*math.sin(y*math.pi/30.0)) * 2.0/3.0
#         return ret
#     def transform_lng(x, y):
#         ret = 300.0 + x + 2.0*y + 0.1*x*x + 0.1*x*y + 0.1*math.sqrt(abs(x))
#         ret += (20.0*math.sin(6.0*x*math.pi) + 20.0*math.sin(2.0*x*math.pi)) * 2.0/3.0
#         ret += (20.0*math.sin(x*math.pi) + 40.0*math.sin(x/3.0*math.pi)) * 2.0/3.0
#         ret += (150.0*math.sin(x/12.0*math.pi) + 300.0*math.sin(x/30.0*math.pi)) * 2.0/3.0
#         return ret
#     dlat = transform_lat(lng - 105.0, lat - 35.0)
#     dlng = transform_lng(lng - 105.0, lat - 35.0)
#     rad_lat = lat / 180.0 * math.pi
#     magic = math.sin(rad_lat)
#     magic = 1 - ee * magic * magic
#     sqrt_magic = math.sqrt(magic)
#     dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrt_magic) * math.pi)
#     dlng = (dlng * 180.0) / (a / sqrt_magic * math.cos(rad_lat) * math.pi)
#     return lng + dlng, lat + dlat
#
# def gcj02_to_bd09(lng, lat):
#     z = math.sqrt(lng*lng + lat*lat) + 0.00002 * math.sin(lat * math.pi * 3000.0 / 180.0)
#     theta = math.atan2(lat, lng) + 0.000003 * math.cos(lng * math.pi * 3000.0 / 180.0)
#     bd_lng = z * math.cos(theta) + 0.0065
#     bd_lat = z * math.sin(theta) + 0.006
#     return bd_lng, bd_lat
#
# def wgs84_to_bd09(lng, lat):
#     gcj = wgs84_to_gcj02(lng, lat)
#     return gcj02_to_bd09(gcj[0], gcj[1])
#
# def safe_int_str(val):
#     if val is None or val == '未知' or val == '':
#         return '未知'
#     try:
#         return str(int(float(val)))
#     except (ValueError, TypeError):
#         return str(val)
#
# # ====================== 路由 ======================
# @app.route('/')
# def index():
#     return render_template('index.html')
#
# @app.route('/map.html')
# def map_page():
#     center_lat = 37.642706
#     center_lng = 116.800286
#     return render_template('map.html', center_lat=center_lat, center_lng=center_lng, baidu_ak=BAIDU_AK)
#
# @app.route('/query')
# def query():
#     time_str = request.args.get('time')
#     if not time_str:
#         return jsonify({"error": "请提供时间参数，例如 ?time=07:30"}), 400
#
#     try:
#         parts = time_str.split(':')
#         h = int(parts[0])
#         m = int(parts[1])
#         target = datetime(2022, 5, 7, h, m, 0)
#     except:
#         return jsonify({"error": "时间格式错误，请使用 HH:MM 或 HH:MM:SS"}), 400
#
#     start = target - timedelta(seconds=30)
#     end = target + timedelta(seconds=30)
#     mask = (df0507['GPSSHIJIAN_'] >= start) & (df0507['GPSSHIJIAN_'] <= end)
#     window_df = df0507[mask].copy()
#
#     if window_df.empty:
#         return jsonify({"features": [], "count": 0})
#
#     window_df['time_diff'] = abs((window_df['GPSSHIJIAN_'] - target).dt.total_seconds())
#     idx = window_df.groupby('CHEHAO')['time_diff'].idxmin()
#     result = window_df.loc[idx]
#
#     features = []
#     for _, row in result.iterrows():
#         bd_lng, bd_lat = wgs84_to_bd09(row['LNG'], row['LAT'])
#         color = "green" if row['SUDU'] > 0 else "red"
#         features.append({
#             "type": "Feature",
#             "geometry": {"type": "Point", "coordinates": [bd_lng, bd_lat]},
#             "properties": {
#                 "plate": str(int(row['CHEHAO'])),
#                 "speed": float(row['SUDU']),
#                 "line": safe_int_str(row.get('XIANLU', '未知')),
#                 "color": color
#             }
#         })
#
#     return jsonify({"type": "FeatureCollection", "features": features, "count": len(features)})
#
# # ====================== 大屏接口 ======================
# @app.route('/api/indicators')
# def indicators():
#     time_str = request.args.get('time', '12:00')
#     h, m = map(int, time_str.split(':'))
#     target = datetime(2022, 5, 7, h, m, 0)
#     start = target - timedelta(seconds=30)
#     end = target + timedelta(seconds=30)
#     mask = (df0507['GPSSHIJIAN_'] >= start) & (df0507['GPSSHIJIAN_'] <= end)
#     window = df0507[mask]
#
#     if window.empty:
#         return jsonify({
#             "online_count": 0, "top5_lines": [],
#             "avg_speed": 0, "traffic_status": "通畅",
#             "status_dist": {"moving": 0, "parking": 0, "offline": 90}
#         })
#
#     # M-01 在线车辆数
#     online = window['CHEHAO'].nunique()
#
#     # M-02 线路 Top5
#     line_counts = window.groupby('XIANLU')['CHEHAO'].nunique().sort_values(ascending=False).head(5)
#     top5 = [{"line": safe_int_str(k), "count": int(v)} for k, v in line_counts.items()]
#
#     # M-03 平均速度与路况
#     moving = window[window['SUDU'] > 0]['SUDU']
#     avg_speed = round(moving.mean(), 1) if len(moving) > 0 else 0
#     if avg_speed > 35:
#         traffic = "通畅"
#     elif avg_speed >= 25:
#         traffic = "缓行"
#     else:
#         traffic = "拥堵"
#
#     # M-04 状态分布 —— 修正：按每辆车窗口内最后一次上报的状态分类
#     # 先按时间排序，然后对每辆车取最后一条记录
#     window_sorted = window.sort_values('GPSSHIJIAN_')
#     last_record = window_sorted.groupby('CHEHAO').tail(1)
#     moving_count = (last_record['SUDU'] > 0).sum()
#     parking_count = (last_record['SUDU'] == 0).sum()
#     offline_count = 90 - online
#
#     return jsonify({
#         "online_count": online,
#         "top5_lines": top5,
#         "avg_speed": avg_speed,
#         "traffic_status": traffic,
#         "status_dist": {
#             "moving": int(moving_count),
#             "parking": int(parking_count),
#             "offline": int(offline_count)
#         }
#     })
#
#
# @app.route('/api/vehicles')
# def vehicles():
#     time_str = request.args.get('time', '12:00')
#     h, m = map(int, time_str.split(':'))
#     target = datetime(2022, 5, 7, h, m, 0)
#     start = target - timedelta(seconds=30)
#     end = target + timedelta(seconds=30)
#     mask = (df0507['GPSSHIJIAN_'] >= start) & (df0507['GPSSHIJIAN_'] <= end)
#     window = df0507[mask].copy()
#
#     if window.empty:
#         return jsonify({"features": [], "count": 0})
#
#     window['time_diff'] = abs((window['GPSSHIJIAN_'] - target).dt.total_seconds())
#     idx = window.groupby('CHEHAO')['time_diff'].idxmin()
#     result = window.loc[idx]
#
#     features = []
#     for _, row in result.iterrows():
#         bd_lng, bd_lat = wgs84_to_bd09(row['LNG'], row['LAT'])
#         color = "green" if row['SUDU'] > 0 else "red"
#         features.append({
#             "type": "Feature",
#             "geometry": {"type": "Point", "coordinates": [bd_lng, bd_lat]},
#             "properties": {
#                 "plate": str(int(row['CHEHAO'])),
#                 "speed": float(row['SUDU']),
#                 "line": safe_int_str(row.get('XIANLU', '未知')),
#                 "color": color
#             }
#         })
#
#     return jsonify({"type": "FeatureCollection", "features": features, "count": len(features)})
#
#
# @app.route('/api/alerts')
# def alerts():
#     time_str = request.args.get('time', '12:00')
#     h, m = map(int, time_str.split(':'))
#     target = datetime(2022, 5, 7, h, m, 0)
#     start = target - timedelta(seconds=30)
#     end = target + timedelta(seconds=30)
#     mask = (df0507['GPSSHIJIAN_'] >= start) & (df0507['GPSSHIJIAN_'] <= end)
#     window = df0507[mask]
#
#     if window.empty:
#         return jsonify([])
#
#     alerts_list = []
#     for _, row in window.iterrows():
#         if row['SUDU'] > 60:
#             alerts_list.append({
#                 "time": str(row['GPSSHIJIAN_']),
#                 "type": "超速",
#                 "plate": str(int(row['CHEHAO'])),
#                 "speed": float(row['SUDU']),
#                 "line": safe_int_str(row.get('XIANLU', '未知'))
#             })
#         elif 0 < row['SUDU'] < 20:
#             alerts_list.append({
#                 "time": str(row['GPSSHIJIAN_']),
#                 "type": "低速",
#                 "plate": str(int(row['CHEHAO'])),
#                 "speed": float(row['SUDU']),
#                 "line": safe_int_str(row.get('XIANLU', '未知'))
#             })
#
#     return jsonify(alerts_list[:50])
#
#
# if __name__ == '__main__':
#     app.run(debug=True)
# from flask import Flask, render_template, request, jsonify
# import redis
# import json
# import subprocess
# import os
#
# app = Flask(__name__)
#
# # ====================== 连接 Redis ======================
# r = redis.Redis(host='192.168.13.129', port=6379, decode_responses=True)
#
# # ====================== 大屏接口 ======================
# @app.route('/api/indicators')
# def indicators():
#     online_count = r.get('bus:indicators:online_count') or '0'
#     top5_lines_raw = r.get('bus:indicators:top5_lines') or '[]'
#     avg_speed_raw = r.get('bus:indicators:avg_speed') or '0|通畅'
#     status_dist_raw = r.get('bus:indicators:status_dist') or '{"moving":0,"parking":0,"offline":90}'
#
#     parts = avg_speed_raw.split('|')
#     avg_speed = parts[0]
#     traffic_status = parts[1] if len(parts) > 1 else '通畅'
#
#     return jsonify({
#         "online_count": int(online_count),
#         "top5_lines": json.loads(top5_lines_raw),
#         "avg_speed": float(avg_speed),
#         "traffic_status": traffic_status,
#         "status_dist": json.loads(status_dist_raw)
#     })
#
# @app.route('/api/vehicles')
# def vehicles():
#     keys = r.keys('bus:latest:*')
#     features = []
#     for key in keys:
#         data_str = r.get(key)
#         if not data_str: continue
#         data = json.loads(data_str)
#         features.append({
#             "type": "Feature",
#             "geometry": {"type": "Point", "coordinates": [data.get('lng', 0), data.get('lat', 0)]},
#             "properties": {
#                 "plate": data.get('chehao', ''),
#                 "speed": data.get('speed', 0),
#                 "line": data.get('line', '未知'),
#                 "color": "green" if data.get('status') == 'moving' else 'red'
#             }
#         })
#     return jsonify({"type": "FeatureCollection", "features": features, "count": len(features)})
#
# @app.route('/api/alerts')
# def alerts():
#     alert_list = r.lrange('bus:alerts:list', 0, 49)
#     alerts = [json.loads(a) for a in alert_list if a]
#     return jsonify(alerts)
#
# # ====================== 模拟器控制 ======================
# simulator_process = None
#
# @app.route('/api/simulator/start', methods=['POST'])
# def start_simulator():
#     global simulator_process
#     data = request.get_json()
#     start_time = data.get('start_time', '2022-05-07 07:30:00')
#     speed = data.get('speed', 10)
#
#     # ★ 自动清空 Redis，避免旧数据残留
#     r.flushall()
#
#     if simulator_process and simulator_process.poll() is None:
#         simulator_process.terminate()
#         simulator_process.wait()
#
#     simulator_path = os.path.join(os.path.dirname(__file__), '..', 'data-simulator', 'simulator.py')
#     cmd = ['python', simulator_path, '--kafka', '192.168.13.129:9092', '--speed', str(speed), '--start-time', start_time]
#     simulator_process = subprocess.Popen(cmd)
#     return jsonify({"status": "ok", "message": f"模拟器已启动: {start_time}, 倍速: {speed}x"})
#
# @app.route('/api/simulator/stop', methods=['POST'])
# def stop_simulator():
#     global simulator_process
#     if simulator_process and simulator_process.poll() is None:
#         simulator_process.terminate()
#         simulator_process.wait()
#         return jsonify({"status": "ok", "message": "模拟器已停止"})
#     return jsonify({"status": "ok", "message": "模拟器未在运行"})
#
# @app.route('/')
# def index():
#     return render_template('index.html')
#
# if __name__ == '__main__':
#     app.run(debug=True)
from flask import Flask, render_template, request, jsonify
import redis
import json

app = Flask(__name__)

r = redis.Redis(host='192.168.13.129', port=6379, decode_responses=True)

@app.route('/api/indicators')
def indicators():
    online_count = r.get('bus:indicators:online_count') or '0'
    top5_lines_raw = r.get('bus:indicators:top5_lines') or '[]'
    avg_speed_raw = r.get('bus:indicators:avg_speed') or '0|通畅'
    status_dist_raw = r.get('bus:indicators:status_dist') or '{"moving":0,"parking":0,"offline":90}'

    parts = avg_speed_raw.split('|')
    avg_speed = parts[0]
    traffic_status = parts[1] if len(parts) > 1 else '通畅'

    return jsonify({
        "online_count": int(online_count),
        "top5_lines": json.loads(top5_lines_raw),
        "avg_speed": float(avg_speed),
        "traffic_status": traffic_status,
        "status_dist": json.loads(status_dist_raw)
    })

@app.route('/api/vehicles')
def vehicles():
    keys = r.keys('bus:latest:*')
    features = []
    for key in keys:
        data_str = r.get(key)
        if not data_str:
            continue
        data = json.loads(data_str)
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [data.get('lng', 0), data.get('lat', 0)]},
            "properties": {
                "plate": data.get('chehao', ''),
                "speed": data.get('speed', 0),
                "line": data.get('line', '未知'),
                "color": "green" if data.get('status') == 'moving' else 'red'
            }
        })
    return jsonify({"type": "FeatureCollection", "features": features, "count": len(features)})

# ★ 独立的时间接口，只从 Redis 读取 bus:latest_time
@app.route('/api/latest_time')
def latest_time():
    t = r.get('bus:latest_time')
    return jsonify({"time": t or ""})

# 原告警接口保留（但 Flink 已不写入，前端也不会请求）
@app.route('/api/alerts')
def alerts():
    alert_list = r.lrange('bus:alerts:list', 0, 49)
    alerts = [json.loads(a) for a in alert_list if a]
    return jsonify(alerts)

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)