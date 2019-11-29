#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys

key={}


# input comes from STDIN
for line in sys.stdin:
    line = line.strip()
    #from mapper.py
    province, value = line.split('\t')
    itemid, action = value.split(',')
    sell=int(action)
    if province in key:
        goods_click=key[province][0]
        goods_buy=key[province][1]
        goods_click[itemid]=goods_click.get(itemid,0)+1
        if sell == 2:
            goods_buy[itemid]=goods_buy.get(itemid,0)+1

    elif province not in key:
        key[province]=[{},{}]
        goods_click=key[province][0]
        goods_buy=key[province][1]
        goods_click[itemid]=goods_click.get(itemid,0)+1
        if sell == 2:
            goods_buy[itemid]=goods_buy.get(itemid,0)+1

for province in key:
    goods_click=key[province][0]
    goods_buy=key[province][1]
    click_sorted = sorted(goods_click.items(),key=lambda x: x[1],reverse=True)
    buy_sorted = sorted(goods_buy.items(),key=lambda x: x[1],reverse=True)
    print(province+':'+'\n')
    print('该省关注前十产品:')
    for i in range(10):
        print(click_sorted[i],end=" ")
    print('\n')
    print('该省购买前十产品：')
    for k in range(10):
        print(buy_sorted[k],end=" ")
    print('\n')
