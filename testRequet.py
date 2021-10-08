import requests

#Токен
token = ''
#Базовый адрес
base_url='http://rtpiapi.hrdn.io/'

request_headers = {'Authorization': f'Bearer {token}',
                    'Content-Type': 'text/csv',
                    'Range-Unit': 'items'
                  }

request_url = f"{base_url}rtpi_price_page?and=(date_last_in_stock.gte.2021-09-01, rosstat_id.not.is.null),rtpi_price(*)?stock_status.eq.InStok"

response = requests.get(request_url, headers=request_headers)

decoded_content = response.content.decode('utf-8')


print(response)