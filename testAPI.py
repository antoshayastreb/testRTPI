import asyncio
from config import token, base_url
import aiohttp
from asyncio.queues import Queue
import time


#Получение указанного количества записей из указанной таблицы
#**data - параметры:
# Range - кол-во записей 
async def getContentAsync(token: str, base_url: str, table_name: str, **data):
        
        range = '0-0'

        if len(data) != 0 and 'range' in data.keys:
                range = data.values['range']
        if len(data) != 0 and'Range' in data.keys:
                range = data.values['Range']

        request_headers = {'Authorization': f'Bearer {token}',
                    'Content-Type': 'application/json',
                    'Range-Unit': 'items',
                    'Range' : f'{range}'
                  }

         #Адрес запроса
        request_url = base_url + f"{table_name}"

        async with aiohttp.ClientSession(headers= request_headers) as session:
            #Get запрос по адресу
            async with session.get(request_url) as resp:
                        response = await resp.json()
                        return [table_name, response]

async def worker(name, queue: Queue):
    #Получение названия таблицы из очереди
    table_name = await queue.get()

    print(f'{name} getting content of table {table_name}')

    #Ожидание результата работы getExactCountAsync
    result = await getContentAsync(token, base_url, table_name)

    #Уведомление очереди о завершении работы над текущим объектом
    queue.task_done()

    print(f'{name} done, for table {table_name}')

    return result

async def main():
    #Время начала
    begin_time = time.monotonic()
    #Создание очереди
    queue = asyncio.Queue()
    #list таблиц
    tables = ['rtpi_price_page', 'rtpi_price', 'rtpi_product_name', 'rtpi_store_id', 'rtpi_rosstat_weight']
    #Заполнение очереди из списка таблиц
    for table in tables:
        queue.put_nowait(table)
    #Массив задач
    tasks = []
    for idx, val in enumerate(tables):
        task = asyncio.create_task(worker(f'worker-{idx}', queue))
        tasks.append(task)
    #Ожидание обработки всех элементов очереди 
    await queue.join()
    #Отмена задач (сразу после обработки всех элементов в очереди)
    for task in tasks:
        task.cancel()
    #Ожидание отмены всех задач
    result = await asyncio.gather(*tasks, return_exceptions=True)
    #Время завершения
    end_time = time.monotonic() - begin_time

    print(result)
    print(f'estimated time {end_time:.2f} seconds')



asyncio.run(main())