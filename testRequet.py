import json
import pandas as pd
import requests
from config import token, base_url


request_headers = {'Authorization': f'Bearer {token}',
                    'Content-Type': 'application/json',
                    'Range-Unit': 'items',
                    'Range' : '0-0',
                    'Prefer' : 'count=exact'
                  }

#Рабочий, но join не работает

# request_url = f'{base_url}rtpi_price_page?' \
# f'select=*,rtpi_product_name(product_name)&' \
# f'date_last_in_stock=gte.2021-10-11&rosstat_id=not.is.null'

request_url = f'{base_url}rtpi_product_name?'\
f'select=*&web_price_id=in.(27,30)' \

response = requests.get(request_url, headers=request_headers)

#decoded_content = response.content.decode('utf-8')

decoded_content = response.json()

if (response.status_code // 100 != 2):
  print(decoded_content)
else:
  pdObj = pd.DataFrame(decoded_content, columns=["web_price_id", "rtpi_product_name", "price_url", "date_add", "date_last_in_stock", "rosstat_id", "contributor_id", "store_id"])
  pdObj.to_csv('list.csv', sep = "\t", index=False)
  #pdObj.to_excel('list.xlsx', engine='xlsxwriter')
  