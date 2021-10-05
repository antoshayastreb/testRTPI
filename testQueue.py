import asyncio
import aiohttp
from asyncio.queues import Queue
import time

#Токен
token = ''
#Базовый адрес
base_url='http://rtpiapi.hrdn.io/'
#"Воркер" для заупска фукции "getExactCountAsync"
async def worker(name, queue: Queue):
    #Получение названия таблицы из очереди
    table_name = await queue.get()

    print(f'{name} getting exactcount of table {table_name}')

    #Ожидание результата работы getExactCountAsync
    result = await getExactCountAsync(token, base_url, table_name)

    #Уведомление очереди о завершении работы над текущим объектом
    queue.task_done()

    print(f'{name} done, result for table {table_name} is {result}')                     
#Асинхронный запрос для получения кол-ва записей по таблице table_name
async def getExactCountAsync(token, base_url, table_name):
        #Словарь заголовка
        request_headers = {'Authorization': f'Bearer {token}',
                    'Content-Type': 'application/json',
                    'Range-Unit': 'items',
                    'Range' : '0-1',
                    'Prefer' : 'count=exact'
                  }
        #Адрес запроса
        request_url = base_url + f"{table_name}"

        async with aiohttp.ClientSession(headers= request_headers) as session:
            #Get запрос по адресу
            async with session.get(request_url) as resp:
                if (resp.status != 206):
                    print(resp.json())
                    return -1

                try:
                    #Получение range из хедера response
                    range = resp.headers.get('content-Range')
                    return (int)((range.split('/'))[1]) 
                except ValueError:
                    print(resp.json())
                    return -1
                                 
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
    await asyncio.gather(*tasks, return_exceptions=True)
    #Время завершения
    end_time = time.monotonic() - begin_time

    print(f'estimated time {end_time:.2f} seconds')

asyncio.run(main())
