import pandas as pd
from kafka import KafkaProducer
import json
import time
import argparse
import pandas as pd
from kafka import KafkaProducer
import redis as redis_lib
import json
import time
import argparse

def safe_str(val):
    if pd.isna(val) or str(val).strip() == '' or str(val) == '未知':
        return '未知'
    try:
        return str(val).encode('utf-8', errors='replace').decode('utf-8')
    except:
        return '未知'

def main():
    parser = argparse.ArgumentParser(description='宁津县公交GPS数据模拟器')
    parser.add_argument('--csv', type=str, default='D:/Document/公交车/清洗后数据_全量合并.csv')
    parser.add_argument('--kafka', type=str, default='192.168.13.129:9092')
    parser.add_argument('--topic', type=str, default='bus_gps_raw')
    parser.add_argument('--speed', type=float, default=1)
    parser.add_argument('--start-time', type=str, default=None)
    parser.add_argument('--end-time', type=str, default=None)
    # ★ 新增 --redis 参数，用于自动清空 Redis
    parser.add_argument('--redis', type=str, default=None,
                        help='Redis 地址，如 192.168.13.129:6379，指定后会自动清空 Redis')
    args = parser.parse_args()

    # ====================== 自动清空 Redis ======================
    if args.redis:
        redis_host = args.redis.split(':')[0]
        redis_port = int(args.redis.split(':')[1]) if ':' in args.redis else 6379
        try:
            r = redis_lib.Redis(host=redis_host, port=redis_port)
            r.flushall()
            print(f"[模拟器] 已清空 Redis ({redis_host}:{redis_port})")
        except Exception as e:
            print(f"[模拟器] 清空 Redis 失败: {e}")
    # ============================================================

    print(f"[模拟器] 读取CSV: {args.csv}")
    df = pd.read_csv(
        args.csv, encoding='gb18030', parse_dates=['GPSSHIJIAN_'],
        dtype={'JIASHIYUAN': str}
    )
    print(f"[模拟器] 全量数据 {len(df)} 条")

    if args.start_time:
        start_dt = pd.Timestamp(args.start_time)
    else:
        start_dt = df['GPSSHIJIAN_'].min()
        print(f"[模拟器] 未指定起始时间，默认从最早记录开始: {start_dt}")

    df = df[df['GPSSHIJIAN_'] >= start_dt]
    if args.end_time:
        end_dt = pd.Timestamp(args.end_time)
        df = df[df['GPSSHIJIAN_'] <= end_dt]
    df = df.sort_values('GPSSHIJIAN_')
    print(f"[模拟器] 筛选后 {len(df)} 条待发送")

    print(f"[模拟器] 连接 Kafka: {args.kafka}")
    producer = KafkaProducer(
        bootstrap_servers=args.kafka,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print(f"[模拟器] 倍速: {args.speed}x, 开始发送到 Topic: {args.topic}")
    count = 0
    prev_time = None
    start_real = time.time()

    for _, row in df.iterrows():
        if prev_time is not None:
            delta = (row['GPSSHIJIAN_'] - prev_time).total_seconds()
            if delta > 0:
                time.sleep(delta / args.speed)
        prev_time = row['GPSSHIJIAN_']

        msg = {
            "chehao": str(int(row['CHEHAO'])),
            "gpsTime": row['GPSSHIJIAN_'].strftime('%Y-%m-%d %H:%M:%S'),
            "lat": float(row['WEIDU']) / 600000.0,
            "lng": float(row['JINGDU']) / 600000.0,
            "speed": float(row['SUDU']),
            "line": f"{int(float(row['XIANLU']))}路" if pd.notna(row['XIANLU']) and row['XIANLU'] != '未知' else '未知',
            "station": safe_str(row.get('ZHANDIANMINGCHENG'))
        }

        producer.send(args.topic, msg)
        count += 1

        if count % 1000 == 0:
            elapsed = time.time() - start_real
            print(f"  已发送 {count} 条, 当前时间 {row['GPSSHIJIAN_']}, 速率 {count/elapsed:.1f} 条/秒")

    producer.flush()
    producer.close()
    elapsed = time.time() - start_real
    print(f"[模拟器] 发送完毕, 共 {count} 条, 总耗时 {elapsed:.1f} 秒")

if __name__ == '__main__':
    main()