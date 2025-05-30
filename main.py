import requests
import pandas as pd
import time
import os
from datetime import datetime

# 配置信息
API_HOST = "47.116.74.96:8087"
API_ENDPOINT = "/api/v1/sensor/latest/bycustomer"
CUSTOMER_ID = 3238
AUTH_TOKEN = "ips neimeng"
CSV_FILENAME = "sensor_data.csv"
INTERVAL_SECONDS = 20  # 每分钟采集一次


def fetch_sensor_data():
    """从API获取传感器数据"""
    try:
        url = f"http://{API_HOST}{API_ENDPOINT}?Id={CUSTOMER_ID}"
        headers = {"Authorization": AUTH_TOKEN}

        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # 检查HTTP错误

        data = response.json()
        if data.get('Code') != 0:
            print(f"API返回错误: {data.get('Message', '未知错误')}")
            return None

        return data['Data']

    except requests.exceptions.RequestException as e:
        print(f"请求失败: {str(e)}")
        return None
    except ValueError:
        print("响应不是有效的JSON格式")
        return None


def transform_to_dataframe(raw_data):
    """将原始数据转换为DataFrame"""
    if not raw_data:
        return None

    # 当前采集时间
    collection_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    records = []
    try:
        # 客户信息
        customer_id = raw_data['Id']
        customer_name = raw_data['Name']
        customer_address = raw_data['Address']

        # 遍历所有设备
        for equipment in raw_data['Equipments']:
            equipment_id = equipment['Id']
            equipment_name = equipment['Name']
            equipment_number = equipment['Number']

            # 遍历设备上的所有传感器
            for sensor in equipment['Sensors']:
                sensor_id = sensor['Id']
                sensor_addr = sensor['Addr']
                sensor_title = sensor['Title']
                sensor_type = sensor['Type']

                # 遍历传感器的所有测量值
                for measure in sensor['Measures']:
                    record = {
                        '采集时间': collection_time,
                        '客户ID': customer_id,
                        '客户名称': customer_name,
                        '客户地址': customer_address,
                        '设备ID': equipment_id,
                        '设备名称': equipment_name,
                        '设备编号': equipment_number,
                        '传感器ID': sensor_id,
                        '传感器地址': sensor_addr,
                        '传感器名称': sensor_title,
                        '传感器类型': sensor_type,
                        '测量类型': measure['Type'],
                        '测量项': measure['Title'],
                        '测量值': measure['Value'],
                        '单位': measure['Unit'],
                        '数据时间戳': measure['Timestamp']
                    }
                    records.append(record)

        return pd.DataFrame(records)

    except KeyError as e:
        print(f"数据转换错误，缺少字段: {str(e)}")
        return None


def save_to_csv(df, filename):
    """将数据保存到CSV文件，并在每次采集后添加空行"""
    if df is None or df.empty:
        print("无有效数据可保存")
        return

    try:
        # 检查文件是否存在
        file_exists = os.path.isfile(filename)

        # 保存数据到CSV
        df.to_csv(filename, mode='a', index=False, header=not file_exists)
        print(f"成功保存{len(df)}条数据到{filename}")

        # 在数据后添加一个空行作为分隔
        with open(filename, 'a', encoding='utf-8') as f:
            f.write('\n')  # 添加一个空行

    except Exception as e:
        print(f"保存数据失败: {str(e)}")


def main():
    """主函数，每分钟采集一次数据"""
    print(f"传感器数据采集程序启动 (每 {INTERVAL_SECONDS} 秒采集一次)")
    print(f"数据将保存到: {CSV_FILENAME}")

    # 创建CSV文件并写入表头（如果文件不存在）
    if not os.path.isfile(CSV_FILENAME):
        print("创建新的CSV文件并写入表头")
        # 创建空DataFrame只包含表头
        columns = [
            '采集时间', '客户ID', '客户名称', '客户地址',
            '设备ID', '设备名称', '设备编号',
            '传感器ID', '传感器地址', '传感器名称', '传感器类型',
            '测量类型', '测量项', '测量值', '单位', '数据时间戳'
        ]
        pd.DataFrame(columns=columns).to_csv(CSV_FILENAME, index=False)

    try:
        while True:
            start_time = time.time()
            print(f"\n开始采集数据 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")

            # 获取数据
            raw_data = fetch_sensor_data()

            if raw_data:
                # 转换为DataFrame
                df = transform_to_dataframe(raw_data)

                if df is not None and not df.empty:
                    # 保存到CSV（包含空行间隔）
                    save_to_csv(df, CSV_FILENAME)
                else:
                    print("未获取到有效数据")
            else:
                print("API请求未返回有效数据")

            # 计算并等待下一轮采集
            elapsed = time.time() - start_time
            wait_time = max(INTERVAL_SECONDS - elapsed, 1)

            print(f"本次采集耗时: {elapsed:.2f}秒")
            print(f"等待 {wait_time:.2f}秒后继续...")
            time.sleep(wait_time)

    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"程序发生错误: {str(e)}")


if __name__ == "__main__":
    main()