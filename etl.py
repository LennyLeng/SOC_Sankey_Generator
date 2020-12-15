#coding=utf8

import pandas as pd
import json
import re

def get_filter(csv_file):
    filter_list = []
    data = pd.read_csv(csv_file, encoding='gbk', header=0)

    for data_row in data.values:
        if (data_row[0].startswith('#')):
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
    # 读取csv文件，thousands用于处理千分位分割符，header用于指定第一行为表头
    data = pd.read_csv(csv_file, encoding='gbk', header=0, thousands=',')
    #按照事件数量降序排列
    data = data.sort_values(by='事件数',ascending=False)

    # 生成打环判定源地址集合,集合set具有不重复特征
    i0_set_tmp = set()
    i1_set_tmp = set()

    links_tmp = {}
    nodes_tmp = set()
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

            #删除首尾空白不可见符
            data_row[0] = (str)(data_row[0])
            data_row[0] = data_row[0].strip()

            data_row[1] = (str)(data_row[1])
            data_row[1] = data_row[1].strip()

            data_row[2] = (str)(data_row[2])
            data_row[2] = data_row[2].strip()

            #处理事件名称中的字符空格
            data_row[2] = data_row[2].replace(" ", "")

            #filter list policy 开始
            is_exclude = False
            for filter in filter_list:
                try:
                    if(filter['filter_type'] == 'ex' and re.search(filter['pattern_val'], data_row[filter['col_index']])):
                        print(data_row[0], '\t=>\t', data_row[2][:10] + '…', '\t=>\t', data_row[1], '\t|\t',str(data_row[3]),
                              '\t',filter['tip'],'\t忽略')
                        is_exclude = True
                        break

                    if (filter['filter_type'] == 'in' and not re.search(filter['pattern_val'],data_row[filter['col_index']])):
                        print(data_row[0], '\t=>\t', data_row[2][:10] + '…', '\t=>\t', data_row[1], '\t|\t',
                              str(data_row[3]),
                              '\t', '未'+filter['tip'], '\t忽略')
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
                i1_set_tmp.add(data_row[1])

            if (data_row[0] == '空值'):
                print(data_row[0], '\t=>\t', data_row[2][:10] + '…', '\t=>\t', data_row[1], '\t|\t', str(data_row[3]),
                      '\t源为空\t修正')
                data_row[0] = '0.0.0.0'
                i0_set_tmp.add(data_row[0])

            if (data_row[1] in i0_set_tmp ):
                print(data_row[0], '\t=>\t', data_row[2][:10] + '…', '\t=>\t', data_row[1], '\t|\t', str(data_row[3]),
                      '\t出现打环情况\t忽略')
                continue

            if (data_row[0] in i1_set_tmp ):
                print(data_row[0], '\t=>\t', data_row[2][:10] + '…', '\t=>\t', data_row[1], '\t|\t', str(data_row[3]),
                      '\t出现多级情况\t忽略')
                continue

            i0_set_tmp.add(data_row[0])
            i1_set_tmp.add(data_row[1])

            nodes_tmp.add(data_row[0])
            nodes_tmp.add(data_row[1])
            nodes_tmp.add(data_row[2])

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

if __name__ == '__main__':
    filter_list = get_filter('conf/filter.csv')
    json_data = generate_json('csv/test.csv', '', filter_list)
    with open('web/tmp.json', 'w') as f:
        f.write(json_data)