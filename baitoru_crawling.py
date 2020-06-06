import requests
import time
from bs4 import BeautifulSoup
import re
import os
import shutil
from tqdm import tqdm

"""
都市ごとのurl

東京23区：https://www.baitoru.com/kanto/jlist/tokyo/23ku/
大阪市：https://www.baitoru.com/kansai/jlist/osaka/osakashi/
横浜市：https://www.baitoru.com/kanto/jlist/kanagawa/yokohamashi/
さいたま市：https://www.baitoru.com/kanto/jlist/saitama/saitamashi/
名古屋市：https://www.baitoru.com/tokai/jlist/aichi/nagoyashi/
"""

def crawling():
    
    occupation_list = ['販売', 'フード・飲食', 'サービス', 'イベント', '軽作業・物流', '工場・製造', 
                       '建築・土木', '営業', 'オフィス', 'IT・クリエイティブ/クリエイター', '教育', 
                       '医療・介護・福祉', '美容・理容・サロン', '調査・モニター', '専門職、その他', 
                       'ガールズバー・キャバクラ・スナック']

    occupation_url = ['sales', 'food', 'service', 'event', 'lightwork', 'production', 'architecture', 
                      'business', 'office', 'creative', 'education', 'medicalcare', 'beauty', 
                      'investigation', 'professional', 'nightwork']

    # 都市名と都市ごとのurlの配列
    citys = ['東京23区', '大阪市', '横浜市', 'さいたま市', '名古屋市']
    citys_eng = ['23ku', 'osaka', 'yokohama', 'saitama', 'nagoya']

    urls = [r'https://www.baitoru.com/kanto/jlist/tokyo/23ku/',
            r'https://www.baitoru.com/kansai/jlist/osaka/osakashi/',
            r'https://www.baitoru.com/kanto/jlist/kanagawa/yokohamashi/',
            r'https://www.baitoru.com/kanto/jlist/saitama/saitamashi/',
            r'https://www.baitoru.com/tokai/jlist/aichi/nagoyashi/']

    # ファイルを保存するディレクトリを作る
    # もしすでに存在していたら消去する
    crawled_dir = "./crawled_file/"
    if os.path.exists(crawled_dir):
        shutil.rmtree(crawled_dir)
    os.mkdir(crawled_dir)

    # 都市ごとにクローリングを行う
    for index in range(len(citys)):
        print(citys[index], "をクローリング中\n")
        base_url = urls[index]

        # 都市ごとのファイルを保存するディレクトリ
        city_name = citys_eng[index]
        city_dir = crawled_dir + f"{city_name}/"

        os.mkdir(city_dir)

        # 職種ごとにクローリングを行う
        for i in range(len(occupation_list)):

            # 職種ごとの基本となるurl
            base_url_o = base_url + occupation_url[i] + '/btp1/'

            # 一旦1ページ目を取得し、総ページ数を取得する
            r = requests.get(base_url_o)
            soup = BeautifulSoup(r.content, "lxml")

            # 総ページ数は文字列なので整数型に変換する
            # 数字にはコンマ(,)が入っていることがあるので正規表現を使い削除する
            total_job = soup.find(id="js-job-count").text    #総アルバイト数
            total_job = int(re.sub("\\D", "", total_job))

            # バイトが掲載されていない場合は処理をやめる
            if total_job == 0:
                break

            # ページ数が5ページ以下だとエラーとなるため、総アルバイト数から総ページ数を算出する
            try:
                total_page = soup.find("li", class_="last").text    #総ページ数
                total_page = int(re.sub("\\D", "", total_page))
            except AttributeError:
                total_page = total_job // 20 + 1
            
            print(f"{occupation_list[i]}")

            bar = tqdm(total=total_page)

            # 職種ごとのファイルを保存するディレクトリ
            city_occupation_dir = city_dir + f"{occupation_url[i]}/"
            os.mkdir(city_occupation_dir)

            with open(f'{city_occupation_dir}{city_name}_{occupation_url[i]}_1.html', 'w') as f:
                f.write(r.text)

            bar.update(1)

            # 2ページ目以降の全てのページをhtmlファイルとしてに保存
            for j in range(2, total_page+1):
                url = base_url_o + "page" + str(j)
                r = requests.get(url)

                with open(f'{city_occupation_dir}{city_name}_{occupation_url[i]}_{j}.html', 'w') as f:
                    f.write(r.text)

                time.sleep(1)    #クローリング間隔は1秒とする

                bar.update(1)


if __name__ == '__main__':
    crawling()