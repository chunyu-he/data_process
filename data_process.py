import re
import logging
import time
import sys
import mysql.connector
import mysql.connector.pooling

# 常量定义
DELIMITER = "----"
PHONE_NUM_PATTERN = r"1[3456789]\d{9}"

# 默认参数值
DEFAULT_BATCH_SIZE = 1000
DEFAULT_POOL_SIZE = 5

# 日志记录器配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

start_line = 158739000

# 数据库信息
INSERT_TABLE = "info_data_1"
MYSQL_HOST = "192.168.2.41"
# MYSQL_HOST = "192.168.0.113"
MYSQL_USER = "root"
MYSQL_PASSWORD = "123456"

"""
数据处理
"""


def data_process(data_file, batch_size=DEFAULT_BATCH_SIZE):
    # 提交的批次
    batch_num = int(int(start_line) / int(DEFAULT_BATCH_SIZE))
    # 要插入的数据
    insert_values = []

    # 创建连接池
    try:
        cnxpool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="mypool",
            pool_size=DEFAULT_POOL_SIZE,
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database="mydata"
        )

    except Exception as e:
        logging.error("创建连接池失败，请检查Mysql服务状态。")
        raise

    with open(data_file, "r", encoding="ISO-8859-1") as data:
        for current_line, line in enumerate(data, start=1):
            if int(current_line) < int(start_line):
                continue
            # 去除行尾回车换行
            line = line.strip()
            # 解析第一列数据
            columns = line.split(DELIMITER)
            first_column = columns[0]
            second_column = columns[1] if len(columns) > 1 else None

            # 判断是否为手机号码，如果是，则将它到插入数据中的phone_numbers字段中
            phone_nums = re.findall(PHONE_NUM_PATTERN, first_column)
            if phone_nums:
                account = second_column
                phone_numbers = phone_nums[0]

                if not account or len(account) > 20 or not phone_numbers or len(phone_numbers) != 11:
                    continue
                else:
                    # errorlog=str(account)+"----"+str(phone_numbers)
                    # logging.error( errorlog)
                    # logging.error(f"第{current_line}行数据异常，已丢弃")
                    insert_values.append((account, None, phone_numbers))
            else:
                account = first_column
                password = second_column

                if not account or len(account) > 20 or not password or len(password) > 20:

                    continue
                else:
                    # errorlog = str(account) + "----" + str(password)
                    # logging.error(errorlog)
                    # logging.error(f"第{current_line}行数据异常，已丢弃")
                    insert_values.append((account, password, None))
            # 如果达到了指定的批次大小，则向数据库提交数据
            if len(insert_values) >= batch_size:
                try:
                    cnx = cnxpool.get_connection()
                    cursor = cnx.cursor()
                    sql = "INSERT INTO " + INSERT_TABLE + " (account, passwd, phone_numbers) VALUES (%s, %s, %s)"
                    cursor.executemany(sql, insert_values)
                    cnx.commit()
                    logging.info(f"第{batch_num}批次数据写入成功：{len(insert_values)}条")
                    batch_num += 1
                    insert_values.clear()
                except mysql.connector.IntegrityError as e:
                    logging.error(f"第{batch_num}批次数据写入失败:数据重复")
                    failed_task_tolog(insert_values)
                    # raise

                finally:
                    logging.error(f"第{batch_num}批次数据写入失败")
                    cursor.close()
                    cnx.close()

                # if len(insert_values) >= batch_size:
                #     cnx = cnxpool.get_connection()
                #     cursor = cnx.cursor()
                #     sql = "INSERT INTO " + INSERT_TABLE + " (account, passwd, phone_numbers) VALUES (%s, %s, %s)"
                #     cursor.executemany(sql, insert_values)
                #     cnx.commit()
                #
                #     logging.error(f"第{batch_num}批次数据写入失败")
                #     failed_task_tolog(insert_values)
                #
                #     cursor.close()
                #     cnx.close()



        # 提交最后一批不足batch_size的数据
        if insert_values:
            try:
                cnx = cnxpool.get_connection()
                cursor = cnx.cursor()
                sql = "INSERT INTO " + INSERT_TABLE + " (account, passwd, phone_numbers) VALUES (%s, %s, %s)"
                cursor.executemany(sql, insert_values)
                cnx.commit()

            except Exception as e:
                logging.error(f"最后一批数据写入失败：{e}")
                failed_task_tolog(insert_values)
                raise

            finally:
                cursor.close()
                cnx.close()

            logging.info(f"最后一批数据写入成功：{len(insert_values)}条")


def error_task_tolog(insert_values):
    with open("error.txt", "a", encoding="ISO-8859-1") as f:
        f.write(str(insert_values))
        f.write("\n")


def failed_task_tolog(insert_values):
    with open("failed.txt", "a", encoding="ISO-8859-1") as f:
        for line in insert_values:
            f.write(str(line))
            f.write("\n")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(f"需要指定解析的数据位置和从哪行开始处理，示例：python {sys.argv[0]} G:\Mydata\data.txt 1000")
        exit()

    data_file = sys.argv[1]
    start_line = sys.argv[2]
    # data_file = "G:/BaiduNetdiskDownload/16e/data"
    logging.info(f"开始处理数据文件 {data_file}")
    logging.info(f"连接池数:{DEFAULT_POOL_SIZE}")
    logging.info(f"提交量级:{DEFAULT_BATCH_SIZE}")
    logging.info(f"数据库账号:{MYSQL_USER}")
    logging.info(f"数据库密码:{MYSQL_PASSWORD}")
    logging.info(f"数据库地址:{MYSQL_HOST}")
    logging.info(f"表:{INSERT_TABLE}")
    logging.info(f"开始处理")
    time.sleep(3)

    # data_process(data_file)
    try:
        data_process(data_file)
    except Exception as e:
        logging.error(f"处理数据文件 {data_file} 出错：{e}")
        # exit()
