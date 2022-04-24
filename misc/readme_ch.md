# fastapi-queue
[![fury](https://img.shields.io/pypi/v/fastapi-queue.svg)](https://pypi.org/project/fastapi-queue/)
[![licence](https://img.shields.io/github/license/GoodManWEN/fastapi-queue)](https://github.com/GoodManWEN/fastapi-queue/blob/master/LICENSE)
[![pyversions](https://img.shields.io/pypi/pyversions/fastapi-queue.svg)](https://pypi.org/project/fastapi-queue/)
[![Publish](https://github.com/GoodManWEN/fastapi-queue/workflows/Publish/badge.svg)](https://github.com/GoodManWEN/fastapi-queue/actions?query=workflow:Publish)
[![Build](https://github.com/GoodManWEN/fastapi-queue/workflows/Build/badge.svg)](https://github.com/GoodManWEN/fastapi-queue/actions?query=workflow:Build)
[![Docs](https://readthedocs.org/projects/fastapi-queue/badge/?version=latest)](https://readthedocs.org/projects/fastapi-queue/)

Python实现的基于 `Redis` 的快速任务队列，可以起到削峰作用以在互联网应用中保护你的数据后端。

## 什么是 fastapi-queue?
Fastapi-queue 是一个高性能的基于 `Redis` 的，供 `FastAPI` 使用的任务队列，以便他们根据后端实际性能被延迟执行。这意味着当有瞬间高流量涌入你的应用时，你不必担心他们会压垮你的后端数据服务。

## 为什么选择 fastapi-queue?
这个库是给那些想要享受任务队列的好处，但不想让应用依赖结构变得过于复杂的人而设计的。比如想要启用请求队列，但不想在应用链条中再添加`RabbitMQ`，那么使用本库的话可以让应用仅依赖于 Python 和 `Redis` 环境而达到同等效果。

## 特性
- 分离的网关和业务节点。
- 良好的水平扩展性。
- 基于全异步架构，速度非常快。

## 依赖
- fastapi
- aioredis >= 2.0.0
- ThreadPoolExecutorPlus >= 0.2.2
- msgpack >= 1.0.0

## 安装

    pip install fastapi-queue

## 文档
[https://fastapi-queue.readthedocs.io](https://fastapi-queue.readthedocs.io) \(待完善\)

## 响应顺序结构示意图

![](https://raw.githubusercontent.com/goodmanwen/fastapi-queue/main/misc/Schematic_ch.png)
由左到右依次为网关、业务节点、Redis服务。

## Examples

网关
```python
'''
由 FastAPI 编写的网关程序，仅处理请求是否被允许（考虑到权限等因素）进入
而不处理具体业务逻辑的服务。
'''
from typing import Optional, Any
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi_queue import DistributedTaskApplyManager
import aioredis


app = FastAPI()
redis = aioredis.Redis.from_url("redis://localhost")


def get_response(success_status: bool, result: Any) -> JSONResponse | dict:
    if success_status:
        return {"status": 200, "data": result}
    if result == -1:
        return JSONResponse(status_code=503, content="Service Temporarily Unavailable")
    else:
        return JSONResponse(status_code=500, content="Internal Server Error")


@app.get('/')
async def root(request: Request):
    success_status: bool = False
    async with DistributedTaskApplyManager(
        redis = redis, 
        request_path = request.url.path,
    ) as dtmanager:
        if not dtmanager.success(): 
            # 该返回值证明该路由已达到全局请求允许量上限，任务被直接拒绝而不会被加入队列。
            return JSONResponse(status_code=503, content="Service Temporarily Unavailable")
        success_status, result = await dtmanager.rclt(form_data = {}, task_level = 0)
    return get_response(success_status, result)


@app.get('/sync-test')
async def sync_test(request: Request, x: int):
    success_status: bool = False
    async with DistributedTaskApplyManager(
        redis = redis, 
        request_path = request.url.path,
    ) as dtmanager:
        if not dtmanager.success():
            return JSONResponse(status_code=503, content="Service Temporarily Unavailable")
        success_status, result = await dtmanager.rclt(form_data = {'x': x}, task_level = 0)
    return get_response(success_status, result)

@app.get('/async-test')
async def async_test(request: Request, n: int):
    n = min(n, 80)
    success_status: bool = False
    async with DistributedTaskApplyManager(
        redis = redis, 
        request_path = request.url.path,
    ) as dtmanager:
        if not dtmanager.success():
            return JSONResponse(status_code=503, content="Service Temporarily Unavailable")
        success_status, result = await dtmanager.rclt(form_data = {'n': n}, task_level = 0)
    return get_response(success_status, result)
```

业务节点
```python
'''
以下代码将创建一个4进程，每进程中包含4线程的业务节点，所以它在某一时刻可以同时最大
并行16种业务。由于依赖 Redis 进行状态同步，你可以随意地多开节点以增强负载能力而
不用担心冲突问题。由于节点管理器本身可以管理多个进程和线程，所以同一物理设备理论上
仅需运行一个节点。
'''
from fastapi_queue import QueueWorker
from loguru import logger
import asyncio  
import aioredis
import signal 
import sys 
import os 

queueworker = None

async def async_root(*args):
    return "Hello world."

def sync_prime_number(redis, mysql, x):
    # 范例同步调用，判断输入数 X 是否为质数
    # Redis和mysql客户端被默认输入，自定义参数从第三位开始，仅支持kwargs形式的输入。
    import math, time
    if x == 1:
        return True
    for numerator in range(2, int(math.sqrt(x))):
        if x % numerator == 0:
            return False
    time.sleep(0.2) # 模拟计算耗时
    return True

async def async_fibonacci(redis, mysql, n):
    # 范例同步调用，计算斐波那契数列的第n位。
    # Redis和mysql客户端被默认输入，自定义参数从第三位开始，仅支持kwargs形式的输入。

    # 请注意，跨进程传输中的对象必须可以被 msgpack 序列化，这意味着如果你使用某些
    # 自定义对象，或者像本例中如果出现超大整数，而不做处理直接传输的话，序列化将失败。
    # 这种情况下等同于请求失败（返回http 500）。
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b 
    await asyncio.sleep(0.2) # 模拟计算耗时
    return a 

route_table = {
    '/': async_root,
    '/sync-test': sync_prime_number,
    '/async-test': async_fibonacci,
}

route_table_maximum_concurrency = {
    '/': 9999,
    '/sync-test': 320,
    '/async-test': 1000,
}

async def main(pid, logger):
    global queueworker

    first_time_run = True
    while True:
        run_startup, first_time_run = (True if pid != 0 else False) and first_time_run, False
        redis = aioredis.Redis.from_url("redis://localhost")
        try:
            worker = QueueWorker(
                redis, 
                threads=4, 
                route_table_maximum_concurrency = route_table_maximum_concurrency, 
                allowed_type_limit=None, 
                run_startup=run_startup,
                logger=logger,
            )
            queueworker = worker
            [worker.method_register(name, func) for name, func in route_table.items()]
            await worker.run_serve()
            if worker.closeing():
                break
        except Exception as e:
            debug = True
            if debug:
                raise e
    await redis.close()
    logger.info(f"Pid: {worker.pid}, shutdown")


def sigint_capture(sig, frame):
    if queueworker: queueworker.graceful_shutdown(sig, frame)
    else: sys.exit(1)


if __name__ == '__main__':
    logger.remove()
    logger.add(sys.stderr, level="DEBUG", enqueue=True)
    signal.signal(signal.SIGINT, sigint_capture) # 捕捉 `ctrl+c` 结束信号以实现优雅结束
    for _ in range(3):
        pid = os.fork()
        if pid == 0: break
    asyncio.run(main(pid, logger))
```

## 性能

因框架基于全异步支持，即使单次请求中触发了复杂的进程间调用过程，请求回复速度仍能保持较低延迟。普通状态下以http客户端角度观察，请求回复延迟低于3毫秒。

(图1，延迟与客户端并发数的关系)(待完善)

(图2，没秒最大处理请求能力与业务节点数量的关系)(待完善)

## 备注

- 库内代码都经过小心谨慎的debug，其健壮性可在高压力下长时间工作，但与之相对我并未花费太多时间增加模块的通用型和易用性。这意味着使用过程中如果遇到bug你可能需要自行修改源码解决，当然这些bug应是由于模块化过程中各种作者本人不可查的疏忽造成。
- 框架经过了每秒数千并发连接下持续数小时的压力测试，但是为了达到可靠的后端保护的目的，你仍需要用心地自定义后端业务内允许的最大并发处理能力，这意味着为了达到较好的用户体验，你仍需要自定义队列能缓存的最大长度，而不是交由框架自行解决。为了减轻`Redis`在进程间状态协调的压力，`RateLimiter`是一个可以帮助你在网关层面直接拒绝非正常请求的模块，如果瞬间流量涌入过大，它可以从网关自身的角度首选拒绝一部分显然无法处理的请求，而不必请求全局状态。

例如以下代码,
```python
from fastapi_queue import RateLimiter
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()

...

@app.on_event("startup")
async def startup():
    RateLimiter().porter_run_serve()

...

@app.get("/")
@RateLimiter(bucket = 5000, limits_s = 1000)
async def root(request: Request):  
    '''
    RateLimiter 的两个参数分别代表当前网关实例中令牌桶的最大值，和每秒恢复数量。
    以上文5000\1000为例，这意味着该桶内最大同时保存5000个令牌，每次请求会被取走
    一个令牌，当桶内令牌归零则不再接受新请求，而令牌数每秒会恢复1000。该令牌系统
    仅在实例内维护状态，以限制实例最大推向后端的能力，起到保护的作用。具体请求
    执行效能仍要根据用户设定的该路由的全局缓存能力而定。
    '''
    async with DistributedTaskApplyManager(
        ...
    )
```
