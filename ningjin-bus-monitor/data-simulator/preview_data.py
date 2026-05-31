"""
数据预览工具：不经过 Kafka，直接读取 CSV 并按时间顺序打印数据。
用法：
    python preview_data.py --speed 10 --start-time "2022-05-07 07:30:00"
如果不带 --start-time，则从数据最早的时间开始。
"""
import pandas as pd
import time
import argparse
from datetime import datetime

def main():
    parser = argparse.ArgumentParser(description='CSV数据预览')
    parser.add_argument('--csv', type=str,
                        default='D:/Document/公交车/清洗后数据_全量合并.csv',
                        help='CSV文件路径')
    parser.add_argument('--speed', type=float, default=1, help='倍速')
    parser.add_argument('--start-time', type=str, default=None,
                        help='起始时间，格式 yyyy-MM-dd HH:mm:ss')
    parser.add_argument('--end-time', type=str, default=None,
                        help='结束时间（可选）')
    args = parser.parse_args()

    print(f"读取 CSV: {args.csv}")
    df = pd.read_csv(
        args.csv, encoding='gb18030', parse_dates=['GPSSHIJIAN_'],
        dtype={'JIASHIYUAN': str}   # 避免列类型推断错误
    )
    print(f"总数据量: {len(df)} 条")

    # 确定起始时间
    if args.start_time:
        start_dt = pd.Timestamp(args.start_time)
    else:
        start_dt = df['GPSSHIJIAN_'].min()
        print(f"未指定起始时间，从最早记录开始: {start_dt}")

    # 筛选数据
    df = df[df['GPSSHIJIAN_'] >= start_dt]
    if args.end_time:
        end_dt = pd.Timestamp(args.end_time)
        df = df[df['GPSSHIJIAN_'] <= end_dt]
    df = df.sort_values('GPSSHIJIAN_')
    print(f"待预览数据: {len(df)} 条")

    print(f"倍速: {args.speed}x，开始输出...\n")
    count = 0
    prev_time = None
    start_real = time.time()

    for _, row in df.iterrows():
        # 控制速度
        if prev_time is not None:
            delta = (row['GPSSHIJIAN_'] - prev_time).total_seconds()
            if delta > 0:
                time.sleep(delta / args.speed)
        prev_time = row['GPSSHIJIAN_']

        # 构建显示数据
        try:
            lat = float(row['WEIDU']) / 600000.0
            lng = float(row['JINGDU']) / 600000.0
        except:
            lat, lng = 0, 0
        try:
            speed = float(row['SUDU'])
        except:
            speed = 0

        line_val = row.get('XIANLU', '未知')
        if pd.notna(line_val) and line_val != '未知':
            line_val = f"{int(float(line_val))}路"
        else:
            line_val = '未知'

        station = row.get('ZHANDIANMINGCHENG', '未知')
        if pd.isna(station) or str(station).strip() == '':
            station = '未知'

        # 直接打印
        print(f"[{row['GPSSHIJIAN_']}] 车号:{int(row['CHEHAO'])} 速度:{speed:.2f}km/h "
              f"线路:{line_val} 经纬度:({lat:.6f}, {lng:.6f}) 站点:{station}")

        count += 1
        if count % 1000 == 0:
            elapsed = time.time() - start_real
            print(f"--- 已输出 {count} 条, 速率 {count/elapsed:.1f} 条/秒 ---")

    elapsed = time.time() - start_real
    print(f"\n完成。共 {count} 条，耗时 {elapsed:.1f} 秒。")

if __name__ == '__main__':
    main()