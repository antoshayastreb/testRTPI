import asyncio
import re

from config import token, base_url
import aiohttp
from asyncio.queues import Queue
from connectDB import insert, conectDB



async def getall(table:str, limit:int, **kwargs):

    if kwargs is not None and "filter" in kwargs:
        table_count = await getExactCountAsync(table, filter=kwargs["filter"])
    else:
        table_count = await getExactCountAsync(table)
    

    if table_count == -1:
        print(f'Error! Can\'t get exact count of {table}')
        return

    iterat_count = table_count // limit

    while iterat_count == 0:
        limit -= 100
        if limit == 0:
            iterat_count = table_count
            break
        else:
            iterat_count = table_count // limit

    addit_count = table_count - limit * iterat_count

    queue = asyncio.Queue()

    prev_i = 0
    if limit == 0:
        queue.put_nowait(f"0-{iterat_count}")
    else:
        for i in range(iterat_count):
            if i + 1 == iterat_count:
                queue.put_nowait(f"{prev_i}-{(i+1)*limit - 1}")
                prev_i = (i+1)*limit
                queue.put_nowait(f"{prev_i}-{prev_i+addit_count}")
            else:
                queue.put_nowait(f"{prev_i}-{(i+1)*limit - 1}")
                prev_i = (i+1)*limit

    maxworkers = 10 if queue.qsize() > 10 else queue.qsize()
    
    
    tasks = []
    for w in range(maxworkers):
        if kwargs is not None and "filter" in kwargs:
            task = asyncio.create_task(getall_worker(f'worker-{w}', table_name= table, queue= queue, filter=kwargs["filter"]))
        else:
            task = asyncio.create_task(getall_worker(f'worker-{w}', table_name= table, queue= queue))
        tasks.append(task)

    await queue.join()

    result = await asyncio.gather(*tasks)

    for task in tasks:
        task.cancel()
    
    return result

async def getall_worker(name:str, table_name:str, queue: Queue, **kwargs):
    while True:
        #Получение названия таблицы из очереди
        range = await queue.get()

        print(f'{name} getting content of table {table_name} in range {range}')

        #Ожидание результата работы getExactCountAsync
        if kwargs is not None and "filter" in kwargs:
            result = await getContentAsync(table_name, range, filter=kwargs["filter"])
        else:
            result = await getContentAsync(table_name, range)

        if result is not None:
            result_SQL = []
            res:dict
            for res in result:
                result_SQL.append(tuple(res.values()))
            insert(table_name, result_SQL)
                

        #Уведомление очереди о завершении работы над текущим объектом
        queue.task_done()

        print(f'{name} done range {range} for table {table_name}') 

        return result



#Получение указанного количества записей из указанной таблицы
async def getContentAsync(table_name: str, range:str='0-0', **kwargs):
        
        request_headers = {'Authorization': f'Bearer {token}',
                    'Content-Type': 'application/json',
                    'Range-Unit': 'items',
                    'Range' : f'{range}'
                  }

         #Адрес запроса
        request_url = base_url + f"{table_name}"

        if kwargs is not None and "filter" in kwargs:
            request_url = request_url + kwargs["filter"]

        async with aiohttp.ClientSession(headers= request_headers) as session:
            #Get запрос по адресу
            async with session.get(request_url) as resp:
                        response = await resp.json()
                        return response

#Асинхронный запрос для получения кол-ва записей по таблице table_name
async def getExactCountAsync(table_name: str, **kwargs):
        #Словарь заголовка
        request_headers = {'Authorization': f'Bearer {token}',
                    'Content-Type': 'application/json',
                    'Range-Unit': 'items',
                    'Range' : '0-1',
                    'Prefer' : 'count=exact'
                  }
        #Адрес запроса
        request_url = base_url + f"{table_name}"

        if kwargs is not None and "filter" in kwargs:
            request_url = request_url + kwargs["filter"]

        async with aiohttp.ClientSession(headers= request_headers) as session:
            #Get запрос по адресу
            async with session.get(request_url) as resp:
                if (resp.status // 100 != 2):
                    print(resp.json())
                    return -1

                try:
                    #Получение range из хедера response
                    range = resp.headers.get('content-Range')
                    return (int)((range.split('/'))[1]) 
                except ValueError:
                    print(resp.json())
                    return -1


#asyncio.run(getall('rtpi_price_page', 300000, filter="?select=*&date_last_in_stock=gte.2020-10-17&rosstat_id=not.is.null"))

cur = conectDB()

cur.execute('SELECT COUNT(*) from public.rtpi_price_page')

count = cur.fetchall()[0][0]

cur.close()

iterate_count = count // 100

offset = 100

for i in range(iterate_count):

    cur = conectDB()

    offset += i*100

    cur.execute(f'SELECT web_price_id from public.rtpi_price_page limit {offset} offset 100')

    result = cur.fetchall()
    x = map(list, list(result))
    x = sum(x, [])

    x = tuple(x)

    cur.close()

    large_fileter= f'?select=*&web_price_id=in.{x}'

    asyncio.run(getall('rtpi_product_name', 100, filter=large_fileter))