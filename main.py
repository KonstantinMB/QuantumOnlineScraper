import time, csv
from itertools import count
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime

url = 'https://www.quantumonline.com/search.cfm?tickersymbol='


def get_input_data(url):
    info = pd.read_csv('symbols.csv')
    info = info['Financial Instruments'].tolist()
    list_of_urls = []
    for i in range(0, len(info)):
        first_url = url + info[i] + '&sopt=symbol'
        list_of_urls.append(first_url)
    return list_of_urls


def load_data(url):

        head = ['Stock Symbol', 'Company Name', 'Stock Exchange', 'Cpn Rate/Ann Amt', 'LiqPref/CallPrice',
                'Call Date/Matur Date', 'Moodys/S&P Dated', 'Conversion Shares @Price', 'Distribution Dates',
                '15% Tax Rate', 'Parent Company', 'IPO', 'Total IPO Units', '$ Per', 'Previous Ticker Symbol',
                'Change Symbol Date', 'Market Value']
        head = pd.DataFrame(head).transpose()
        head.to_csv('funds.csv', index=False, header=False)

        for k in range(0, len(url)):
            try:
                # loading HTML code for the URLs...
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko)'
                                  ' Chrome/50.0.2661.102 Safari/537.36'}

                page = requests.get(url[k], headers=headers)
                soup = BeautifulSoup(page.content, 'lxml')

                # gets the needed info from the URL
                tables = soup.find_all('table', bgcolor="#ccff99", width="100%", align="center")
                needed_table = tables[1].find_all('tr')
                nd = needed_table[1].find_all('td')

                # first list of data // table 1:
                attribute = []

                for i in range(0, len(nd)):
                    res = nd[i].text.replace('\n', '').replace('\t', '').replace('\r', '')
                    if res.find("Yahoo"):
                        res = res[0:26]
                        if any(c.isalpha() for c in res):
                            res = res.replace('C', '').replace('l', '').replace('i', '').replace('c', '').replace('k', '')
                    if i == 0:
                        res = res.replace('Chart', '')
                    if i == 5:
                        res = res.replace(', ', ' - ')
                    attribute.append(res)
                if len(nd) == 7:
                    attribute.insert(5, '-')

                # get data for the second info portion:
                tables2 = soup.find('table', bgcolor="#DCFDD7", cellspacing="2", width="800").find_all('center')
                tables3 = soup.find('table', bgcolor="#DCFDD7", cellspacing="2", width="800").find_all('td')
                table_len = len(tables2)

                # loading the symbol & name of the company:
                stock_name = []
                symbol = tables2[1].text[15:22].replace(' ', '')
                name_list = tables2[0].text.split(',')
                name = name_list[0]
                stock_name.append(symbol)
                stock_name.append(name)
                tables4 = soup.find('table', bgcolor="#FFEFB5", width="100%", cellspacing="0", border="2", cellpadding="5")

                # second list of data:
                t1_res = []
                if table_len >= 11:
                    if table_len == 12 or table_len == 14:
                        if tables4 is not None:
                            parent_comp = '-'
                            t1 = tables2[2].find('font').text.replace('\n', '').replace('\t', '').replace('\r', '').split(
                                sep=' - ')
                        else:
                            parent_comp = tables2[3].find('b').text.replace('Go to Parent Company\'s Record ', '').replace('(', '') \
                                .replace(')', '')
                            t1 = tables2[4].find('font').text.replace('\n', '').replace('\t', '').replace('\r', '').split(sep=' - ')
                    else:
                        parent_comp = tables2[2].find('b').text.replace('Go to Parent Company\'s Record ', '').replace('(', '') \
                            .replace(')', '')
                        t1 = tables2[3].find('font').text.replace('\n', '').replace('\t', '').replace('\r', '').split(sep=' - ')

                if table_len <= 10:
                    parent_comp = '-'
                    if table_len == 10:
                        t1 = tables2[3].find('font').text.replace('\n', '').replace('\t', '').replace('\r', '').split(sep=' - ')
                        if len(t1) <= 1:
                            t1 = tables2[2].find('font').text.replace('\n', '').replace('\t', '').replace('\r', '').split(
                                sep=' - ')
                    else:
                        t1 = tables2[2].find('font').text.replace('\n', '').replace('\t', '').replace('\r', '').split(sep=' - ')

                if len(t1) > 1:
                    res = t1[2].split(sep=' @ ')
                else:
                    if len(t1) == 1:
                        res = tables2[4].find('font').text.replace('\n', '').replace('\t', '').replace('\r', '').split(
                            sep=' @ ')
                    else:
                        res = tables2[3].find('font').text.replace('\n', '').replace('\t', '').replace('\r', '').split(sep=' @ ')

                t1_res.append(parent_comp)
                if len(t1) > 1:
                    res[1] = res[1].replace('   ', '').replace(' ', '')
                    t1_res.append(t1[1])
                    t1_res.append(res[0])
                    t1_res.append(res[1])
                else:
                    t1_res.append('-')
                    t1_res.append('-')
                    t1_res.append('-')

                # third list of data:
                if table_len >= 11:
                    t2_res = tables2[4].find('font').text.replace('\n', '').replace('\t', '').replace('\r', '').split(sep=': ')
                if table_len <= 10:
                    t2_res = tables2[3].find('font').text.replace('\n', '').replace('\t', '').replace('\r', '').split(sep=': ')

                for i in range(0, len(t2_res)):
                    if i == 1:
                        t2_res[1] = t2_res[1].replace(' \xa0\xa0\xa0Changed', '')
                        break
                del t2_res[0]  # deleting the first element as it is just a unneeded declaration


                info = tables3[1].find_all('center')[6].text.replace('\n', '').replace('\t', '').replace('\r', '')
                #  fourth list of data:
                if table_len <= 10:
                    t3_res = tables3[1].find_all('center')[6].text.replace('\n', '').replace('\t', '').replace('\r', '').replace(
                        'Market Value ', '')
                if table_len >= 11:
                    if table_len == 12 or table_len == 14:
                        if tables4 is not None:
                            t3_res = tables3[1].find_all('center')[5].text.replace('\n', '').replace('\t', '').replace('\r', '').replace(
                                'Market Value ', '')
                        if info == '':
                            t3_res = tables3[1].find_all('center')[7].text.replace('\n', '').replace('\t', '').replace('\r', '').replace(
                                'Market Value ', '')
                    else:
                        t3_res = tables3[1].find_all('center')[6].text.replace('\n', '').replace('\t', '').replace('\r', '').replace(
                            'Market Value ', '')

                # converting all data into lists
                pd_stock = pd.Series(stock_name)
                pd_attr1 = pd.Series(attribute)
                pd_attr2 = pd.Series(t1_res)

                # some Symbols/Stocks don't have info about this list's topic
                if len(t2_res) > 0:
                    pd_attr3 = pd.Series(t2_res)
                else:
                    pd_attr3 = pd.Series(['-', '-'])

                pd_attr4 = pd.Series(t3_res)

                # ... and adding the symbol and stock name  + them all together
                tot_attr = pd.concat([pd_stock, pd_attr1, pd_attr2, pd_attr3, pd_attr4])
                df = pd.DataFrame(tot_attr)
                result = df.transpose()
                result = result.rename_axis(None)
            except:
                print('Cannot scrape. Look at this URL:', url[k])
            else:
                with open('funds.csv', 'a', newline='') as f:
                        result.to_csv(f, index=False, header=False)

                        print('Loading data...')
        print("Data is loaded!")


# Scraping the data:
if __name__ == '__main__':
    startTime = datetime.now()
    load_data(get_input_data(url))
    print('It took', (datetime.now() - startTime).total_seconds(), ' seconds to download all data.')
