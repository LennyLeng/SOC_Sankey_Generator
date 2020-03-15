#coding=utf8

import pandas as pd
import json
import re
def get_filter(csv_file):
    filter_list = []
    data = pd.read_csv(csv_file, encoding='gbk', header=None)
    for data_row in data.values:
        if (data_row[0].startswith('过滤模式') or data_row[0].startswith('#')):
            continue
        dict = {}
        dict['filter_type'] = data_row[0]
        dict['col_index'] = int(data_row[1])
        dict['pattern_val'] = data_row[2]
        dict['tip'] = data_row[3]
        filter_list.append(dict)
    return filter_list


def generate_json(csv_file, limit, filter_list):
    print('开始抽取整理数据...')
    # 读取csv文件
    data = pd.read_csv(csv_file, encoding='gbk', header=None)

    # 生成源地址集合,排除dns
    i0_tmp = []
    i0_tmp.extend(data[0].unique())
    if '源地址' in i0_tmp:
        i0_tmp.remove('源地址')
    if '10.132.0.118' in i0_tmp:
        i0_tmp.remove('10.132.0.118')
    if '10.132.0.119' in i0_tmp:
        i0_tmp.remove('10.132.0.119')

    links_tmp = {}
    nodes_tmp = []
    links = []
    nodes = []

    # 生成links,nodes_tmp

    i = 0
    for data_row in data.values:
        # data_row[0] => 源地址
        # data_row[1] => 目的地址
        # data_row[2].replace(" ", "") => 事件名称
        # data_row[3] => 事件数
        if (limit == '' or i < int(limit)):
            # 跳过表头
            if (data_row[0] == '源地址' or data_row[1] == '目的地址' or data_row[2].replace(" ", "") == '事件名称'):
                continue

            #处理数量千分占位符
            try:
                data_row[3] = int(data_row[3].replace(',', ''))
            except:
                pass

            data_row[2] = data_row[2].replace(" ", "")

            #filter list policy 开始
            is_exclude = False
            for filter in filter_list:
                try:
                    if(filter['filter_type'] == 'ex' and re.match(filter['pattern_val'], data_row[filter['col_index']])):
                        print(data_row[0], '\t=>\t', data_row[2][:10] + '…', '\t=>\t', data_row[1], '\t|\t',str(data_row[3]),
                              '\t',filter['tip'],'\t忽略')
                        is_exclude = True
                        break
                    if (filter['filter_type'] == 'in' and not re.match(filter['pattern_val'],data_row[filter['col_index']])):
                        print(data_row[0], '\t=>\t', data_row[2][:10] + '…', '\t=>\t', data_row[1], '\t|\t',
                              str(data_row[3]),
                              '\t', filter['tip'], '\t忽略')
                        is_exclude = True
                        break
                except:
                    pass
            if(is_exclude):
                continue
            # filter list policy 结束

            if (data_row[2] == '' or data_row[2] == '空值'):
                print(data_row[0], '\t=>\t', data_row[2][:10] + '…', '\t=>\t', data_row[1], '\t|\t', str(data_row[3]),
                      '\t事件名称为空\t修正')
                data_row[2] = '空名称'

            if (data_row[0] == data_row[1] or data_row[1] == '空值'):
                print(data_row[0], '\t=>\t', data_row[2][:10] + '…', '\t=>\t', data_row[1], '\t|\t', str(data_row[3]),
                      '\t源目相同或者目的为空\t修正')
                data_row[1] = '0.0.0.0'

            if (data_row[1] in i0_tmp):
                print(data_row[0], '\t=>\t', data_row[2][:10] + '…', '\t=>\t', data_row[1], '\t|\t', str(data_row[3]),
                      '\t出现打环情况\t忽略')
                continue

            nodes_tmp.append(data_row[0])
            nodes_tmp.append(data_row[1])
            nodes_tmp.append(data_row[2])

            tmp = list()
            tmp.append(data_row[0])
            tmp.append(data_row[2])
            tmp = json.dumps(tmp)
            if (tmp in links_tmp):
                links_tmp[tmp] += data_row[3]
            else:
                links_tmp[tmp] = data_row[3]

            tmp = list()
            tmp.append(data_row[2])
            tmp.append(data_row[1])
            tmp = json.dumps(tmp)
            if (tmp in links_tmp):
                links_tmp[tmp] += data_row[3]
            else:
                links_tmp[tmp] = data_row[3]

        else:
            #print(data_row[0], data_row[1],data_row[2],data_row[3], '!!!')
            break
        i += 1


    # nodes_tmp去重
    nodes_tmp = list(set(nodes_tmp))

    # 处理nodes_tmp中表头
    if '源地址' in i0_tmp:
        i0_tmp.remove('源地址')
    if '目的地址' in i0_tmp:
        i0_tmp.remove('目的地址')
    if '事件名称' in i0_tmp:
        i0_tmp.remove('事件名称')
    if '事件数' in i0_tmp:
        i0_tmp.remove('事件数')

    # 处理nodes_tmp中空值情况
    if '空值' in nodes_tmp:
        nodes_tmp.remove('空值')
        nodes_tmp.append('0.0.0.0')

    # 生成nodes
    for data_row in nodes_tmp:
        dic = {}
        dic['name'] = data_row
        nodes.append(dic)

    for data_row in links_tmp:
        tmp = json.loads(data_row)
        dic = {}
        dic['source'] = tmp[0]
        dic['target'] = tmp[1]
        dic['value'] = links_tmp[data_row]
        links.append(dic)

    data = {'nodes': nodes, 'links': links}
    print('抽取整理数据完成!')
    return json.dumps(data)

