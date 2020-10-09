#coding=utf8

import etl
import server
import init
import os
import time
import multiprocessing
import traceback

def generate_json_and_write_file(csv_file, limit):
    filter_chang_time = float(0)
    while True:
        try:
            if(filter_chang_time < os.stat('conf/filter.csv').st_mtime):
                filter_list = etl.get_filter('conf/filter.csv')
                json_data = etl.generate_json('csv/' + csv_file, limit, filter_list)
                with open('web/tmp.json', 'w') as f:
                    f.write(json_data)
                    print("web服务已启动，打开浏览器访问http://127.0.0.1:%d" % (server.PORT))
                filter_chang_time = os.stat('conf/filter.csv').st_mtime
        except:
            #当filter文件不存在时，重置文件改变时间
            traceback.print_exc()
            filter_chang_time = float(0)
        time.sleep(1)

def run():
    (csv_file, limit) = init.init()
    json_p = multiprocessing.Process(target=generate_json_and_write_file, args=(csv_file, limit))
    json_p.start()
    http_p = multiprocessing.Process(target=server.run)
    http_p.start()

def debug():
    (csv_file, limit) = init.init()
    http_p = multiprocessing.Process(target=server.run)
    http_p.start()
    generate_json_and_write_file(csv_file, limit)

if __name__ == '__main__':
    multiprocessing.freeze_support()
    run()