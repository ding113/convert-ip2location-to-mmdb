import csv
import ipaddress
import socket
import struct
import logging
from tqdm import tqdm
from netaddr import IPSet, IPRange
from collections import defaultdict
import mmdb_writer

# 设置日志记录
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class IP2LocationConverter:
    def __init__(self, input_file, output_file):
        logging.info("Initializing IP2LocationConverter")
        self.input_file = input_file
        self.output_file = output_file
        self.initialize_writer()

    def initialize_writer(self):
        logging.info("Initializing MMDBWriter")
        try:
            self.writer = mmdb_writer.MMDBWriter(
                4, 
                "IP2Location", 
                languages=["en"], 
                description={"en": "IP2Location DB6 Database"}
            )
            logging.info(f"MMDBWriter initialized for {self.output_file}")
        except TypeError as e:
            logging.error(f"Initialization failed with TypeError: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error during initialization: {str(e)}")
            raise

    def ip_to_int(self, ip):
        return int(ipaddress.IPv4Address(ip))

    def num_to_ip(self, num):
        return socket.inet_ntoa(struct.pack('!I', num))

    def process_csv(self):
        logging.info(f"Processing CSV file: {self.input_file}")
        try:
            total_lines = sum(1 for line in open(self.input_file, 'r', encoding='utf-8')) - 1  # 减去头部
            logging.info(f"Total lines in CSV (excluding header): {total_lines}")
        except Exception as e:
            logging.error(f"Error counting lines in CSV: {str(e)}")
            return

        try:
            with open(self.input_file, 'r', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                header = next(reader)  # 跳过头部
                logging.info(f"CSV header: {header}")

                data = defaultdict(list)

                for row in tqdm(reader, total=total_lines, desc="Processing IP ranges"):
                    start_ip = self.num_to_ip(int(row[0]))
                    end_ip = self.num_to_ip(int(row[1]))

                    # 将相同 country 和 isp 的 IP 范围合并
                    data[(row[2], row[8])].append(IPRange(start_ip, end_ip))

                for (country, isp), ranges in data.items():
                    # 将合并后的 IP 范围插入数据库
                    self.writer.insert_network(IPSet(ranges), {
                        'country': {'iso_code': country, 'names': {'en': row[3]}},
                        'region': {'names': {'en': row[4]}},
                        'city': {'names': {'en': row[5]}},
                        'location': {
                            'latitude': float(row[6]),
                            'longitude': float(row[7])
                        },
                        'isp': isp
                    })

        except Exception as e:
            logging.error(f"Error processing CSV: {str(e)}")
            raise

    def generate_mmdb(self):
        logging.info("Generating MMDB file...")
        self.process_csv()
        self.writer.to_db_file(self.output_file)
        logging.info(f"MMDB file generated: {self.output_file}")

    def validate_mmdb(self):
        logging.info("Validating generated MMDB file...")
        try:
            import maxminddb
            with maxminddb.open_database(self.output_file) as reader:
                test_ip = '8.8.8.8'  # Google's public DNS
                result = reader.get(test_ip)
                if result and 'country' in result:
                    logging.info("MMDB file validation successful.")
                else:
                    logging.error("MMDB file validation failed. No valid data found.")
        except Exception as e:
            logging.error(f"MMDB file validation failed: {str(e)}")

def main():
    input_file = '/path/to/your/csvfile/IP-COUNTRY-REGION-CITY-LATITUDE-LONGITUDE-ISP.CSV'
    output_file = 'ip2location.mmdb'
    
    try:
        logging.info("Starting main process")
        converter = IP2LocationConverter(input_file, output_file)
        converter.generate_mmdb()
        converter.validate_mmdb()
        logging.info("Process completed successfully")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
