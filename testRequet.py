import configparser
import json
import pandas as pd
import requests

config = configparser.ConfigParser()

try:
  config.read("settings.ini")
  token = config["Base"]["token_string"]
except configparser.ParsingError as p:
  print(p)
  token = ""
#Базовый адрес
base_url='http://rtpiapi.hrdn.io/'

request_headers = {'Authorization': f'Bearer {token}',
                    'Content-Type': 'application/json',
                    'Range-Unit': 'items'
                  }

#Рабочий, но join не работает
#request_url = f"{base_url}rtpi_price_page?and=(date_last_in_stock.gte.2021-09-01, rosstat_id.not.is.null)&rtpi_price(*)?stock_status.eq.InStok"

#request_url = f"{base_url}rtpi_price_page?and=(date_last_in_stock.gte.2020-07-01, date_last_in_stock.lte.2020-09-01, rosstat_id.not.is.null),rtpi_price(stock_status)&rtpi_price.stock_status.eq.InStok"

#request_url = f"{base_url}rtpi_product_name?select=product_name,rtpi_price_page(date_last_in_stock, rosstat_id)and=(date_last_in_stock.gte.2021-09-01, rosstat_id.not.is.null),rtpi_price(stock_status)&stock_status.eq.InStok"

#request_url = f"{base_url}rtpi_product_name?select=product_name,web_price_id:web_price_id(date_last_in_stock, rosstat_id)and=(date_last_in_stock.gte.2021-09-01, rosstat_id.not.is.null)"

request_url = f'{base_url}rtpi_price_page?' \
f'select=*,rtpi_product_name(product_name)&' \
f'date_last_in_stock=gte.2021-10-06' 
#f'&rtpi_price_page.order=date_last_in_stock'

response = requests.get(request_url, headers=request_headers)

#decoded_content = response.content.decode('utf-8')

decoded_content = response.json()

if (response.status_code // 100 != 2):
  print(decoded_content)
else:
  pdObj = pd.DataFrame(decoded_content, columns=["web_price_id", "rtpi_product_name", "price_url", "date_add", "date_last_in_stock", "rosstat_id", "contributor_id", "store_id"])
  pdObj.to_csv('list.csv', sep = "\t", index=False)
  #pdObj.to_excel('list.xlsx', engine='xlsxwriter')
  