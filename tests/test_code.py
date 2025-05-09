from re import S
import re
import asyncio ,time


def timeout(func):
    async def wrapper(*args ,**kwargs):
        start = time.time()
        print(f"开始运行 {func.__name__} {start}")
        result = await func(*args ,**kwargs)
        print(f"运行了  {time.time() - start :.2f}s")
        return result
        
    return wrapper

class Async:
    def __init__(self):
        self.tasks = set()
        self.results = []

    @timeout
    async def start(self):
        tasks = [asyncio.create_task(self.test_asyncio(name,k+1)) for k,name in enumerate(["Alice", "Bob", "Charlie"])]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            print("程序被手动终止了")
        finally:
            print(f"程序结束了 {tasks}")
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def stop(self):
        for task in self.tasks:
            await task.cancel()
        print(f"{self.tasks}程序被手动终止了")
        #await asyncio.gather(*self.tasks, return_exceptions=True)   

    async def test_asyncio(self,name :str ,sleep :int):
        while True:
            await self.transation(sleep)


    async def transation(self ,sleep):
        await asyncio.sleep(sleep)
        if sleep == 2:
            print("==2")
            return
    
            
def test(*args):
    print(args)
if __name__ == "__main__":
    #task = Async()
    try:
        result = 30120425528 / 1068710001364596
        print (f"{result:.9f}")
        #asyncio.run(task.start())
    except KeyboardInterrupt:
        print("程序被手动终止了")
        #asyncio.run(task.stop())

   