import pandas as pd
import numpy as np
import re
import os
import shutil
from bs4 import BeautifulSoup
from tqdm import tqdm

def scraping():

    # 都市名の配列
    citys = ['東京23区', '大阪市', '横浜市', 'さいたま市', '名古屋市']
    citys_eng = ['23ku', 'osaka', 'yokohama', 'saitama', 'nagoya']

    # 検収確認
    check = [[], [], [], [], []]

    # csvファイルを保存するディレクトリを作る
    # もしすでに存在していたら消去する
    result_dir = "./result_file/"
    if os.path.exists(result_dir):
        shutil.rmtree(result_dir)
    os.mkdir(result_dir)

    for index in range(len(citys)):

        # データフレームを初期化する
        df = pd.DataFrame(columns=['企業名', '最寄り駅', '職種', '給与形態', '給与金額', '勤務開始時間', '勤務終了時間', 
                                   '日払い', '週払い', '高収入', '学生', '高校生', 'ミドル', '主婦(夫)', '未経験OK', 
                                   '交通費有', '年齢(10代)', '年齢(20代)', '年齢(30代)', '年齢(40代)', '年齢(50代)', 
                                   '男女割合', '仕事の仕方', '職場の様子'])

        # スクレイピングをする都市名
        city_name = citys_eng[index]

        print(f"\n{citys[index]}をスクレイピング中\n")

        # 1つ目を取り出し、総アルバイト数と総ページ数を取得する
        with open(f"./crawled_file/{city_name}/{city_name}_1.html") as f:
            soup = BeautifulSoup(f, "lxml")
        total_job = soup.find(id="js-job-count").text    #総アルバイト数
        total_page = soup.find("li", class_="last").text    #総ページ数

        # 総アルバイト数と総ページ数は文字列なので整数型に変換する
        # 数字にはコンマ(,)が入っていることがあるので正規表現を使い削除する
        total_job = int(re.sub("\\D", "", total_job))
        total_page = int(re.sub("\\D", "", total_page))

        check[index].append(total_job)

        bar = tqdm(total=total_page)

        df = acquisit(df, soup)
        bar.update(1)

        # 残りのページも取得する
        for i in range(2, total_page+1): #全ページ
            baitoru_file = open(f"./crawled_file/{city_name}/{city_name}_{i}.html", "r")
            baitoru = baitoru_file.read()
            baitoru_file.close()
            soup = BeautifulSoup(baitoru, 'lxml')
            df = acquisit(df, soup)
            bar.update(1)

        check[index].append(len(df))

        # csvファイルに保存する
        df.to_csv(f'{result_dir}{city_name}.csv', index=False)
        print(citys[index], "完了\n")

    print("検収確認（取得アルバイト数/元データ）")
    print("＿＿＿＿＿＿＿＿＿＿")
    for i in range(5):
        print("{}：{}/{}".format(citys[i], check[i][1], check[i][0]))



# 変数を取得し、引数のDataFrameに追加する関数
# データが存在しない場合はtry構文でnanを与える
def acquisit(df, soup):

    #各求人をページ毎にリストに保存
    job_list = soup.find_all("article", class_="list-jobListDetail")

    # アルバイトの特徴
    ch = ['日払い', '週払い', '高収入', '学生', '高校生', 'ミドル', '主婦(夫)', '未経験OK', '交通費有']

    for job in job_list: #ページ内の求人ごとに

        # 企業名
        try:
            corp = job.find_all(class_="pt02b")[0].find("p").text
            corp = re.sub("/.*", "", corp)
        except AttributeError:
            corp = np.nan

        # 最寄り駅
        try:
            station = job.find_all(class_="ul02")[1].find("span").text
            station = re.search(r'.*駅', station).group()
        except AttributeError:
            station = np.nan

        # 職種
        try:
            occupation = job.find(class_="pt03").find_all("dd")[0].text.strip()

            # [ア・パ][契]のような職種を削除する
            occupation = re.sub("\[[^\]]*\]", "", occupation)

            # 複数ある場合は一つ目の職種だけ取得する
            if occupation[:3] == "①②③":
                occupation = re.sub("①②③", "", occupation)
            elif occupation[:2] == "①②":
                occupation = re.sub("①②|、③.*", "", occupation)
            elif occupation[0] == "①":
                occupation = re.sub("①|、②.*", "", occupation)
        except AttributeError:
            occupation = np.nan


        # 給与
        try:
            pay = job.find("div", class_="pt03").find_all("dd")[1].find("em").text
            # 最初に書かれている給与だけ抜き出す
            pay = pay[:pay.find("円")]
            if pay == "":
                pay_form = "完全出来高制"
                pay_qty = np.nan
            pay = re.sub("①|②|③|,", "", pay)
            pay_form = pay[:2]
            pay_qty = int(pay[2:])
        except AttributeError:
            pay_form = np.nan
            pay_qty = np.nan

        # 勤務時間
        try:
            time = job.find(class_="pt03").find_all("dd")[2].text.strip()
            time = re.sub("\[[^\]]*\]", "", time)
            # 複数ある場合は一つ目の時間帯だけ取得する
            if time[:3] == "①②③":
                time = re.sub("①②③", "", time)
            elif time[:2] == "①②":
                time = re.sub("①②|、③.*", "", time)
            elif time[0] == "①":
                time = re.sub("①|、②.*", "", time)

            # 開始時刻と終了時刻に分ける
            start_time = int(time[:2]) + int(time[3:5]) / 60
            end_time = int(time[-5:-3]) + int(time[-2:]) / 60

        except AttributeError:
            start_time = np.nan
            end_time = np.nan

        
        # 特徴
        characteristics = str(job.find(class_="pt04").find("ul"))
        ch_length = len(ch)
        ch_value = [0 for i in range(ch_length)]
        for i in range(ch_length):
            if ch[i] in characteristics:
                ch_value[i] = 1

        # 年齢層
        try:
            ages = job.find(class_="dl01").find_all(class_="on")
            age_10 = 0
            age_20 = 0
            age_30 = 0
            age_40 = 0
            age_50 = 0
            for age in ages:
                age = int(re.sub("代", "", age.text))
                if age == 10:
                    age_10 = 1
                elif age == 20:
                    age_20 = 1
                elif age == 30:
                    age_30 = 1
                elif age == 40:
                    age_40 = 1
                elif age == 50:
                    age_50 = 1
                else:
                    raise Exception("年齢層において例外が発生しました")
        except AttributeError:
            age_10 = np.nan
            age_20 = np.nan
            age_30 = np.nan
            age_40 = np.nan
            age_50 = np.nan
        
        # 男女割合
        sex_ratio = value(job, 2)

        # 仕事の仕方
        manner = value(job, 3)

        # 職場の様子
        atmos = value(job, 4)


        # 仮のデータフレームにまとめる
        df_temp = pd.DataFrame({'企業名': corp, '最寄り駅': station, '職種': occupation, '給与形態': pay_form, 
                                '給与金額': pay_qty, '勤務開始時間': start_time, '勤務終了時間': end_time, '日払い': ch_value[0], 
                                '週払い': ch_value[1], '高収入': ch_value[2], '学生': ch_value[3], '高校生': ch_value[4], 
                                'ミドル': ch_value[5], '主婦(夫)': ch_value[6], '未経験OK': ch_value[7], '交通費有': ch_value[8], 
                                '年齢(10代)': age_10, '年齢(20代)': age_20, '年齢(30代)': age_30, '年齢(40代)': age_40, 
                                '年齢(50代)': age_50, '男女割合': sex_ratio, '仕事の仕方': manner, '職場の様子': atmos},
                                index=[0])

        # 統合する
        df = pd.concat([df, df_temp])

    return df


# 男女割合、仕事の仕方、職場の様子の評価値を算出する
def value(job, n):
    try:
        rows = job.find(class_=f"dl0{n}").find("ul").find_all("li")
        value = 0
        length = 0
        for i, row in enumerate(rows):
            row_length = len(str(row))
            if length < row_length:
                length = row_length
                # 4で割って値の範囲が0~1の指標にする
                value = i / 4
    except AttributeError:
        value = np.nan
    return value


if __name__ == '__main__':
    scraping()