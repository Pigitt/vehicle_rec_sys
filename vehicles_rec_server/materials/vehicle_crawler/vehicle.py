import requests
# import json
# # from csv import reader 
# import os
# import sys
from bs4 import BeautifulSoup
# import time
# import random
from lxml import etree
# import os
import re
# import datetime

class Vehicle():
    def __init__(self):
        self.domain = 'https://www.caranddriver.com'
        self.make=[
                # 'Acura',
                # 'Alfa-Romeo',
                # 'Aston-Martin',
                # 'Audi',
                # 'Bentley',
                # 'BMW',
                # 'Bugatti',
                # 'Buick',
                # 'Cadillac',
                # 'Chevrolet',
                # 'Chrysler',
                # 'Dodge',
                # 'Ferrari',
                'Fiat'#,
                # 'Ford',
                # 'Genesis',
                # 'GMC',
                # 'Honda',
                # 'Hyundai',
                # 'Infiniti',
                # 'Jaguar',
                # 'Jeep',
                # 'Kia',
                # 'Lamborghini',
                # 'Land-Rover',
                # 'Lexus',
                # 'Lincoln',
                # 'Lotus',
                # 'Maserati',
                # 'Mazda',
                # 'McLaren',
                # # 'Mercedes-AMG',
                # 'Mercedes-Benz',
                # # 'Mercedes-Maybach',
                # 'Mini',
                # 'Mitsubishi',
                # 'Nissan',
                # 'Polestar',
                # 'Porsche',
                # 'Ram',
                # 'Rolls-Royce',
                # 'Scion',
                # 'Smart',
                # 'Subaru',
                # 'Suzuki',
                # 'Tesla',
                # 'Toyota',
                # 'Volkswagen',
                # 'Volvo'
                ]
    
    def get_specs(self,url):
        response = requests.get(url=url)
        soup= BeautifulSoup(response.text, "lxml")
        specs={}
        for i in soup.select('div[class="css-qgjj1l e17ofjz23"]'):
            # print(i.select('button[class="css-17t01md e17ofjz22"]')[0].text)
            specs[i.select('button[class="css-17t01md e17ofjz22"]')[0].text]={}
            list=[]
            for j in i.select('div[class="css-1ajawdl e2zahha0"]'):
                try:
                    specs[i.select('button[class="css-17t01md e17ofjz22"]')[0].text][j.contents[0].text]=j.contents[1].text
                    # print(j.contents[0].text,j.contents[1].text)
                except IndexError:
                    list.append(j.contents[0].text)
                    # print(j.contents[0].text)
                    specs[i.select('button[class="css-17t01md e17ofjz22"]')[0].text]=list
        return specs
    def get_rates(self,url):
        response = requests.get(url=url)
        soup= BeautifulSoup(response.text, "lxml")
        # html = etree.HTML(response.text)
        # html.xpath()
        rate={}
        try:
            star={}
            for i in soup.select('div[class="eov6l8h1 css-4dxonz ehh6yb86"]'):
                star[i.select('span[class="css-ihxznl ehh6yb82"]')[0].text]=i.select('span[class="css-1c6thby ehh6yb83"]')[0].text
            mark={}
            for i in soup.select('div[class="css-12jjwzq"]'):
                mark[i.select('div[class="css-1rttn8x"]')[0].text]=i.select('div[class="css-sf59yt"]')[0].text
                # print(i.select('div[class="css-1rttn8x"]')[0].text,i.select('div[class="css-sf59yt"]')[0].text)
            rate={
            'overall':soup.select('div[class="css-wti69m"]')[0].text,
            'count':re.findall('.*\([^\)\(\d]*(\d+)[^\)\(\d]*\).*',soup.select('div[class="css-14z3b1p"]')[0].text)[0],
            'recby': soup.select('span[class="css-19pqkoc e1agtnah1"]')[0].text,
            'star':star,
            'mark':mark
            }
        except IndexError:
            pass
        return rate
    def get_data(self):
        vehicle={}
        for m in self.make:
            url_main=self.domain+'/'+m.lower()
            response = requests.get(url=url_main)
            soup=soup= BeautifulSoup(response.text, "lxml")
            href=soup.select('a[class="vehicle-item-title item-title"]')
            # vehicle={}
            for u in href:
                url_each=self.domain+u.get('href')
                # print(url_each)
                response = requests.get(url=url_each)
                soup= BeautifulSoup(response.text, "lxml")
                name=soup.select('h1[class="css-i4j13t e10ise8i2"]')[0].text
                try:
                    year=soup.select('a[class^="current"]')[0].text    
                except IndexError:
                    try :
                        year=soup.select('span[itemprop="name"]')[3].text
                    except IndexError:
                        year=re.findall('(^\d{4})',soup.select('h1[class="css-i4j13t e10ise8i2"]')[0].text)[0]
                make=soup.select('span[itemprop="name"]')[1].text
                model=soup.select('span[itemprop="name"]')[2].text
                # price=soup.select('div[class="css-48aaf9 e1zcv6h1"]')[0].text
                kbb_url='https://www.kbb.com'+'/'+make.replace(' ','-').lower()+'/'+model.replace(' ','-').lower()+'/'+year+'/consumer-reviews'
                vehicle[name]={
                    'name':name,
                    'year':year,
                    'make':make,
                    'model':model}
                try:
                    price=re.findall('\d+',soup.select('span[class="css-1ykuyyb e10ise8i0"]')[0].text.replace(',',''))[0]
                except IndexError:
                    price=''
                vehicle[name]['price']=price
                vehicle[name]['kbb_url']=kbb_url
                vehicle[name]['rate']=self.get_rates(kbb_url)
                try:
                    cnd_url=self.domain+soup.select('#main-content > header > div.css-16uv341.endngjl0 > div.css-70qvj9.e17a0kax2 > div > a[title="Specs"]')[0].attrs['href']
                    vehicle[name]['cnd_url']=cnd_url
                    vehicle[name]['specs']=self.get_specs(cnd_url)
                except IndexError:
                    vehicle[name]['cnd_url']=''
                    vehicle[name]['specs']={}
            # with open(m+'.json', "w") as dump_f:
            #     json.dump(vehicle, dump_f, indent=4)
        yield vehicle