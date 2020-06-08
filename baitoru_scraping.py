import pandas as pd
import numpy as np
import glob
from pathlib import Path
import re
import os
import shutil
from tqdm import tqdm
from bs4 import BeautifulSoup

def scraping():

    # 都市名の配列
    citys = ['東京23区', '大阪市', '横浜市', 'さいたま市', '名古屋市']
    citys_eng = ['23ku', 'osaka', 'yokohama', 'saitama', 'nagoya']

    # 職種の配列
    occupation_list = ['販売', 'フード・飲食', 'サービス', 'イベント', '軽作業・物流', '工場・製造', 
                       '建築・土木', '営業', 'オフィス', 'IT・クリエイティブ/クリエイター', '教育', 
                       '医療・介護・福祉', '美容・理容・サロン', '調査・モニター', '専門職、その他', 
                       'ガールズバー・キャバクラ・スナック']

    occupation_url = ['sales', 'food', 'service', 'event', 'lightwork', 'production', 'architecture', 
                      'business', 'office', 'creative', 'education', 'medicalcare', 'beauty', 
                      'investigation', 'professional', 'nightwork']


    # csvファイルを保存するディレクトリを作る
    # もしすでに存在していたら消去する
    current_dir = Path.cwd()
    result_dir = current_dir / 'result_file'
    if os.path.exists(result_dir):
        shutil.rmtree(result_dir)
    os.mkdir(result_dir)

    # htmlファイルを格納したディレクトリ
    crawled_dir = current_dir / 'crawled_file'

    # 検収確認用の変数
    check = [[0, 0] for i in range(len(citys))]
    check_error = False
    check_error_position = []

    column_name = ['企業名', '最寄り駅', '職種', '給与形態', '給与金額', '勤務開始時間', '勤務終了時間', 
                   '日払い', '週払い', '高収入', '学生', '高校生', 'ミドル', '主婦(夫)', '未経験OK', 
                   '交通費有', '年齢(10代)', '年齢(20代)', '年齢(30代)', '年齢(40代)', '年齢(50代)', 
                   '男女割合', '仕事の仕方', '職場の様子', 'ハローワーク']


    for index in range(len(citys)):

        print(f"{citys[index]}をスクレイピング中")

        # スクレイピングをする都市名
        city_name = citys_eng[index]

        # 都市ごとのhtmlファイルを格納したディレクトリ
        city_crawled_dir = crawled_dir / city_name

        # 保存するcsvファイルのパス
        path_csv = result_dir / f'{city_name}.csv'

        for i in range(len(occupation_list)):

            # データフレームを初期化する
            df = pd.DataFrame(columns=column_name)

            occupation_name_en = occupation_url[i]

            # 1つ目を取り出し、総アルバイト数と総ページ数を取得する
            city_occupation_crawled_dir = city_crawled_dir / occupation_name_en
            html_path = city_occupation_crawled_dir / f'{city_name}_{occupation_name_en}_1.html'
            html_path = Path(html_path)

            occupation_name_ja = occupation_list[i]

            # ファイルが存在しなければその職業にはアルバイトがないということなので次の職種に移す
            try:
                with html_path.open() as f:
                    soup = BeautifulSoup(f, "lxml")
            except FileNotFoundError:
                print(f'{occupation_name_ja}はありませんでした')
                break

 
            # 総アルバイト数と総ページ数は文字列なので整数型に変換する
            # 数字にはコンマ(,)が入っていることがあるので正規表現を使い削除する
            total_job = soup.find(id="js-job-count").text    #総アルバイト数
            total_job = int(re.sub("\\D", "", total_job))

            # ページ数が5ページ以下だとエラーとなるため、総アルバイト数から総ページ数を算出する
            total_page = soup.find("li", class_="last")
            if total_page != None:
                total_page = total_page.text    #総ページ数
                total_page = int(re.sub("\\D", "", total_page))
            else:
                total_page = total_job // 20 + 1

            bar = tqdm(total=total_page)
            bar.set_description(occupation_name_ja)

            df, hw_judge = acquisit(df, soup, occupation_name_ja)

            bar.update(1)

            # 残りのページも取得する
            for j in range(2, total_page+1):
                path_html = city_occupation_crawled_dir / f'{city_name}_{occupation_name_en}_{j}.html'
                f = path_html.open()
                baitoru = f.read()
                f.close()
                soup = BeautifulSoup(baitoru, 'lxml')
                bar.update(1)
                if hw_judge == False:
                    df, hw_judge = acquisit(df, soup, occupation_name_ja)
                else:
                    check[index][0] = (j-1)*20
                    break
            
            job_qty = len(df)
            check[index][1] += job_qty
            difference = check[index][0] - job_qty


            # 1ページ当たり20のアルバイトが掲載されているのでページ数*20と20以上の差があるとおかしい
            if (difference < 0) or (difference >= 20):
                check_error = True
                check_error_position.append(f'{citys[index]}{occupation_name_ja}')

            # メモリを節約するため、職業ごとにcsvファイルに保存する
            # 最初だけヘッダーも書き込む
            if i == 0:
                df.to_csv(path_csv, index=False)
            else:
                df.to_csv(path_csv, mode='a', index=False, header=False)

        print(f"{citys[index]}をcsvファイルに保存しました\n")

    print("検収確認（取得アルバイト数/最大アルバイト数）")
    print("＿＿＿＿＿＿＿＿＿＿＿＿")
    for i in range(len(check)):
        print("{}：{}/{}".format(citys[i], check[i][1], check[i][0]))

    if check_error:
        print('検収条件を満たしませんでした')
        print('以下で数が合っていません')
        for position in check_error_position:
            print(position)

    else:
        print('異常はありませんでした')



# 変数を取得し、引数のDataFrameに追加する関数
# データが存在しない場合はでnanを与える
def acquisit(df, soup, occupation):

    #各求人をページ毎にリストに保存
    job_list = soup.find_all("article", class_="list-jobListDetail")

    # アルバイトの特徴
    ch = ['日払い', '週払い', '高収入', '学生', '高校生', 'ミドル', '主婦(夫)', '未経験OK', '交通費有']

    for job in job_list: #ページ内の求人ごとに

        # 企業名
        corp = job.find_all(class_="pt02b")[0].find("p")
        if corp != None:
            corp = corp.text
            corp = re.sub("/.*", "", corp)
        else:
            corp = np.nan

        # 最寄り駅
        station = job.find_all(class_="ul02")[1].find("span")
        if station != None:
            station = station.text
            station = re.search(r'.*駅', station).group()
        else:
            station = np.nan


        # 給与
        pay = job.find("div", class_="pt03").find_all("dd")[1].find("em")
        if pay != None:
            pay = pay.text
            # 最初に書かれている給与だけ抜き出す
            pay = pay[:pay.find("円")]
            pay = re.sub("①|②|③|,", "", pay)
            
            if "万" in pay:
                pay = re.sub("万", "", pay)
                pay_form = pay[:2]
                pay_qty = int(float(pay[2:])*10000)

            elif pay[1] == "給":
                pay_form = pay[:2]
                pay_qty = int(pay[2:])

            elif pay == "":
                pay_form = "完全出来高制"
                pay_qty = np.nan

            else:
                pay_form = pay[:re.search("\d*$", pay).start()]
                pay_qty = re.search("\d*$", pay).group()
                
        else:
            pay_form = np.nan
            pay_qty = np.nan

        # 勤務時間
        time = job.find(class_="pt03").find_all("dd")[2]
        if time != None:
            time = time.text.strip()
            time = re.sub("\[[^\]]*\]", "", time)
            # 複数ある場合は一つ目の時間帯だけ取得する
            if time[:3] == "①②③":
                time = re.sub("①②③", "", time)
            elif time[:2] == "①③":
                time = re.sub("①③|、②.*", "", time)
            elif time[:2] == "①②":
                time = re.sub("①②|、③.*", "", time)
            elif time[0] == "①":
                time = re.sub("①|、②.*", "", time)

            # 開始時刻と終了時刻に分ける
            start_time = int(time[:2]) + int(time[3:5]) / 60
            end_time = int(time[6:8]) + int(time[9:11]) / 60

        else:
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
        if job.find(class_="dl01") != None:
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
        else:
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

    # job_listが20でないときは最後のファイルか、ハローワークのアルバイトが入っているかのどちらかである
    hw_judge = False
    if len(job_list) != 20:
        hw_judge = True

    return df, hw_judge

"""======================================


# なぜかjob_listが空となり、データを取得できなかったためハローワークの掲載は無視する



# ハローワーク求人の変数を取得し、引数のDataFrameに追加する関数
# データが存在しない場合はでnanを与える
def acquisit_hw(df, soup, occupation):

    # 各求人をページ毎にリストに保存
    job_list = soup.find_all("article", class_="list-jobListDetailH")

    for job in job_list: #ページ内の求人ごとに

        # 企業名
        corp = job.find_all(class_="pt02b")[0].find("p")
        if corp != None:
            corp = corp.text
            corp = re.sub("/.*", "", corp)
        else:
            corp = np.nan

        # 最寄り駅
        station = job.find_all(class_="ul02")[1].find("span")
        if station != None:
            station = station.text
            station = re.search(r'.*駅', station).group()
        else:
            station = np.nan


        # 給与
        pay = job.find("div", class_="pt03").find_all("dd")[0].find("em")
        if pay != None:
            pay = pay.text
            # 最初に書かれている給与だけ抜き出す
            pay = pay[:pay.find("円")]
            pay = re.sub("①|②|③|,", "", pay)
            
            if "万" in pay:
                pay = re.sub("万", "", pay)
                pay = int(float(pay)*10000)

            pay_qty = re.search("\d*$", pay).group()
                
        else:
            pay_qty = np.nan

        # 勤務時間
        time = job.find(class_="pt03").find_all("dd")[1]
        if time != None:
            time = time.text.strip()
            time = re.sub("\[[^\]]*\]", "", time)
            start_time = time
        else:
            start_time = np.nan
        


        # 仮のデータフレームにまとめる
        df_temp = pd.DataFrame({'企業名': corp, '最寄り駅': station, '職種': occupation, '給与形態': np.nan, 
                                '給与金額': pay_qty, '勤務開始時間': start_time, '勤務終了時間': np.nan, '日払い': np.nan, 
                                '週払い': np.nan, '高収入': np.nan, '学生': np.nan, '高校生': np.nan, 
                                'ミドル': np.nan, '主婦(夫)': np.nan, '未経験OK': np.nan, '交通費有': np.nan, 
                                '年齢(10代)': np.nan, '年齢(20代)': np.nan, '年齢(30代)': np.nan, '年齢(40代)': np.nan, 
                                '年齢(50代)': np.nan, '男女割合': np.nan, '仕事の仕方': np.nan, '職場の様子': np.nan, 'ハローワーク': True},
                                index=[0])

        # 統合する
        df = pd.concat([df, df_temp])

    return df
===================================="""


# 男女割合、仕事の仕方、職場の様子の評価値を算出する
def value(job, n):
    rows = job.find(class_=f"dl0{n}")
    if rows != None:
        rows = rows.find("ul").find_all("li")
        value = 0
        length = 0
        for i, row in enumerate(rows):
            row_length = len(str(row))
            if length < row_length:
                length = row_length
                # 4で割って値の範囲が0~1の指標にする
                value = i / 4
    else:
        value = np.nan
    return value


if __name__ == '__main__':
    scraping()