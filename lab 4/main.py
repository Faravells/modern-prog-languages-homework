import pandas as pd
from bs4 import BeautifulSoup
import time
import requests

def parseShips(url):
    response = requests.get(url, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'})
    soup = BeautifulSoup(response.text, 'html.parser')
    str = soup.find('div', class_='pagination-totals')
    if str is None or str == [] or str.get_text() != '1 судно':
        return None
    shipData = {'Название': None, 'IMO': None, 'MMSI': None, 'Тип': None}
    str = soup.find('a', class_='ship-link')
    time.sleep(1)
    response = requests.get('https://www.vesselfinder.com' + str.get_attribute_list(key='href')[0], headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'})
    soup = BeautifulSoup(response.text, 'html.parser')
    shipData['Название'] = soup.find('h1', class_='title').get_text()
    tdArr = soup.find_all('td')
    for i in range(0, len(tdArr)):
        if tdArr[i].get_text() == 'MMSI':
            shipData['MMSI'] = tdArr[i + 1].get_text();
        elif tdArr[i].get_text() == 'IMO / MMSI':
            shipData['IMO'], shipData['MMSI'] = tdArr[i + 1].get_text().split('/')
            shipData['IMO'] = shipData['IMO'].strip()
            shipData['MMSI'] = shipData['MMSI'].strip()
        elif tdArr[i].get_text() == 'AIS тип':
            shipData['Тип'] = tdArr[i + 1].get_text();
    return shipData

def main():
    df_links = pd.read_excel('Links.xlsx')
    res = []
    for index, row in df_links.iterrows():
        print(f'Обработка {index + 1} ссылки')
        info = parseShips(row['Ссылка'])
        if info:
            res.append(info)
    resDf = pd.DataFrame(res)
    resDf.to_excel('result.xlsx', index=False)
    print(f"Сохранено {len(res)} записей в result.xlsx")

if __name__ == '__main__':

    main()
