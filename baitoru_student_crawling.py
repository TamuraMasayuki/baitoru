import requests
import time
from bs4 import BeautifulSoup
import re
import os
import shutil
from tqdm import tqdm

"""
都市ごとのurl

東京23区：https://www.baitoru.com/kanto/jlist/tokyo/23ku/wrd%E5%AD%A6%E7%94%9F/
大阪市：https://www.baitoru.com/kansai/jlist/osaka/osakashi/wrd%E5%AD%A6%E7%94%9F/
横浜市：https://www.baitoru.com/kanto/jlist/kanagawa/yokohamashi/wrd%E5%AD%A6%E7%94%9F/
さいたま市：https://www.baitoru.com/kanto/jlist/saitama/saitamashi/wrd%E5%AD%A6%E7%94%9F/
名古屋市：https://www.baitoru.com/tokai/jlist/aichi/nagoyashi/wrd%E5%AD%A6%E7%94%9F/
"""

def crawling():

    # 都市名と都市ごとのurlの配列
    citys = ['東京23区', '大阪市', '横浜市', 'さいたま市', '名古屋市']
    citys_eng = ['23ku', 'osaka', 'yokohama', 'saitama', 'nagoya']

    urls = [r'https://www.baitoru.com/kanto/jlist/tokyo/23ku/wrd%E5%AD%A6%E7%94%9F/',
            r'https://www.baitoru.com/kansai/jlist/osaka/osakashi/wrd%E5%AD%A6%E7%94%9F/',
            r'https://www.baitoru.com/kanto/jlist/kanagawa/yokohamashi/wrd%E5%AD%A6%E7%94%9F/',
            r'https://www.baitoru.com/kanto/jlist/saitama/saitamashi/wrd%E5%AD%A6%E7%94%9F/',
            r'https://www.baitoru.com/tokai/jlist/aichi/nagoyashi/wrd%E5%AD%A6%E7%94%9F/']

    # ファイルを保存するディレクトリを作る
    # もしすでに存在していたら消去する
    crawled_dir = "./crawled_file/"
    if os.path.exists(crawled_dir):
        shutil.rmtree(crawled_dir)
    os.mkdir(crawled_dir)

    # 都市ごとにクローリングを行う
    for index in range(len(citys)):
        print(citys[index], "をクローリング中")
        base_url = urls[index]

        # 一旦1ページ目を取得し、総アルバイト数と総ページ数を取得する
        r = requests.get(base_url)
        soup = BeautifulSoup(r.content, "lxml")
        total_job = soup.find(id="js-job-count").text    #総アルバイト数
        total_page = soup.find("li", class_="last").text    #総ページ数

        # 総アルバイト数と総ページ数は文字列なので整数型に変換する
        # 数字にはコンマ(,)が入っていることがあるので正規表現を使い削除する
        total_job = int(re.sub("\\D", "", total_job))
        total_page = int(re.sub("\\D", "", total_page))

        # 都市ごとのファイルを保存するディレクトリ
        city_name = citys_eng[index]
        city_dir = crawled_dir + f"{city_name}/"

        os.mkdir(city_dir)

        with open(f'{city_dir}{city_name}_1.html', 'w') as f:
            f.write(r.text)


        # 2ページ目以降の全てのページをhtmlファイルとしてに保存
        bar = tqdm(total=total_page-1)
        for i in range(2, total_page+1):
            url = base_url + "page" + str(i)
            r = requests.get(url)
            time.sleep(1)    #クローリング間隔は1秒とする

            with open(f'{city_dir}{city_name}_{i}.html', 'w') as f:
                f.write(r.text)

            bar.update(1)

        print(citys[index], "のクローリング完了\n")

if __name__ == '__main__':
    crawling()