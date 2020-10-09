#coding=utf8
import etl
import os

def init():
    print("启明星辰SOC日志Sankey图分析生成工具V1.5.1")
    # 1.0   基本功能
    # 1.1   修复snakeyjson数据原到目的的数值不一致问题
    # 1.2   增加白名单排除配置文件
    # 1.3   修复filter正则匹配问题，使用更宽松的search代替match
    # 1.31  修复源ip未空情况
    # 1.4   修复i0源地址集排除逻辑，打环判定更精确；修复http.server存在缓存，刷新数据不是最新的情况；修复filter文件不在时程序报错的情况。
    # 1.5   重构i0源地址集排除逻辑，优化padas读取csv文件逻辑，使用thousands用于处理千分位分割符，使用header用于指定第一行为表头
    # 1.5.1 新增多级排除逻辑，优化padas读取config文件逻辑

    print("作者：Lenny\n")

    # 选择文件
    print('将启明SOC日志文件整理成表头为：[源地址,目的地址,事件名称,事件数]的CSV格式文件，放于csv目录下')
    csv_files = dict()
    data_row = 1
    for file in os.listdir('csv'):
        if (file[-4:] == '.csv'):
            print("[%d] %s" % (data_row, file))
            csv_files[str(data_row)] = file
            data_row += 1
    select_index = input('输入要加载的文件序号[1-%d]:' % (data_row - 1))
    print("已选择:%s" % (csv_files[str(select_index)]))
    # 输入日期
    date = input('输入图表副标题:')
    with open('web/sub_title.ini', 'w', encoding='utf-8') as f:
        f.write(date)

    # 输入提取条数
    limit = input('输入提取条数（不输入默认为全部提取,建议50条）:')

    return (csv_files[str(select_index)], limit)