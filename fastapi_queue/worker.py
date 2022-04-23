import os
import sys
import signal
import asyncio
import aioredis
import msgpack as json
from async_timeout import timeout
from typing import Optional
try
    from typing import Literal
except:
    from typing_extensions import Literal
from functools import partial
from ThreadPoolExecutorPlus import ThreadPoolExecutor
from .utils import PseudoThreadPoolExecutor
try:
    import uvloop
    uvloop.install()
except:
    ...

maximum_subthread = min(256, os.cpu_count() * 16)
debug = False

class QueueWorker:

    def __init__(
        self, 
        redis: aioredis.client,  
        threads: int = 1,
        route_table_maximum_concurrency: dict = None,
        allowed_type_limit: Optional[int] = None, 
        run_startup: bool = False,
        logger = None,
    ):
        '''
        Use task levels to mark how much time a task may consume, so that some nodes may only process low time consuming tasks to avoid a scenario where a large number of long time consuming tasks in the queue take up all the resources.

        The range of `allowed_type_limit` is an integer from 0 to 9. ()
        For example, `allowed_type_limit` is set to 2, then it will accept level 0, 1 and 2 tasks and reject tasks above level 3.
        '''
        self.redis = redis
        self.mysql = None
        self._thread_num = threads
        self._worker_thread_futs = []
        self._thread_ready_status = []
        self._worker_thread_full_list = []
        self._inbound_query_channel = 'channel:query'
        self._query_queue_name = 'queue:web'
        self._allowed_type_limit = allowed_type_limit
        self._run_startup = run_startup
        self._method_map = {}
        self._route_table_maximum_concurrency = route_table_maximum_concurrency
        self._result_channel_prefix = 'channel:result:'
        self._confirm_token = b'confirmed'
        self._queue_of_query = "queue:web"
        self._token_prefix = b'routelimit:'
        self._close_token = b'halt'
        self._fully_occupied = False
        self._closing = False
        self._has_synchronou_process = False
        self._close_flag_lst = {'main':0, 'close':0, "ltrigger":0}
        self._logger = logger
        self._close_flag = asyncio.Future()
        self.pid = os.getpid()
        self.startup()


    def startup(self):
        '''
        Synchronization code executed at startup.
        '''
        if self._allowed_type_limit is not None:
            self._queue_script = self.redis.register_script(f'local lstlength=tonumber(redis.call("llen", KEYS[1]))-1;local buffer=nil;if lstlength > 200 then local broadcast=redis.call("lrange", KEYS[1], 0, -1);for idx, str in ipairs(broadcast) do if string.byte(string.sub(str,1,1)) <= {48 + self._allowed_type_limit} then buffer = str;redis.call("lset", KEYS[1], idx-1, "0");redis.call("lrem", KEYS[1], 1, "0");break;end;end ;return buffer; else for idx=0, lstlength do local str = redis.call("lindex", KEYS[1], idx);if string.byte(string.sub(str,1,1)) <= {48 + self._allowed_type_limit} then buffer = str;redis.call("lset", KEYS[1], idx, "0");redis.call("lrem", KEYS[1], 1, "0");break;end;end;return buffer;end')
        asyncio.create_task(self._loop_trigger())


    async def async_startup(self):
        '''
        Asynchronous scripts executed at startup, not necessarily executed, controlled by parameter `run_startup: bool`, False by default.
        '''
        keys = await self.redis.keys()
        for key in keys:
            if key[:len(self._token_prefix)] == self._token_prefix:
                await self.redis.delete(key)
        await self.redis.ltrim(self._queue_of_query, 1, 0)
        for path, limit in self._route_table_maximum_concurrency.items():
            await self.redis.set(self._token_prefix+path.encode(), limit)


    def method_register(self, method_route, method):
        '''
        A very primitive way to distinguish between synchronous and asynchronous functions, it is to determine whether their function names start with `sync_` or `async_`, with synchronous functions being added to the executor to avoid blocking threads. Simple, but can be effective in improving performance.
        '''
        if method.__name__[0] == 's':
            self._has_synchronou_process = True 
        self._method_map[method_route] = method


    def closeing(self):
        return self._closing


    def _query_string_parser(self, byte_string: bytes) -> (bytes, str, str, dict):
        task_level, left_bytes = byte_string[0], byte_string[2:]
        task_route_index = left_bytes.index(b':')
        task_route, left_bytes = left_bytes[:task_route_index].decode(), left_bytes[task_route_index + 1:]
        task_uuid_index = left_bytes.index(b':')
        task_uuid, left_bytes = left_bytes[:task_uuid_index].decode(), left_bytes[task_uuid_index + 1:]
        form_data = json.loads(left_bytes)
        return task_level, task_route, task_uuid, form_data


    async def _waiting_new_query(self, channel: aioredis.client.PubSub):
        while True:
            self._logger.debug(f"Pid: {self.pid}, blocking and waiting to listen.")
            await self._start_listen_fut
            try:
                self._logger.debug(f"Pid: {self.pid}, start listening and hang up the program.")
                message = await channel.get_message(ignore_subscribe_messages=True, timeout=3600)
                self._logger.debug(f"Pid: {self.pid}, take out message: {message}.")
                if message is not None:
                    self._logger.debug(f"Pid: {self.pid}, current subthread occupancy status: {self._fully_occupied}:{self._thread_ready_status}.")
                    if not self._fully_occupied:
                        if self._closing:
                            for idx in range(self._thread_num):
                                if self._thread_ready_status[idx] == 1:
                                    self._thread_ready_status[idx] = 0
                                    self._worker_thread_futs[idx].set_result(False)
                            return
                        else:
                            for idx in range(self._thread_num):
                                if self._thread_ready_status[idx] == 1: # The thread is available
                                    self._thread_ready_status[idx] = 0
                                    self._worker_thread_futs[idx].set_result(None)
                                    break
                        self._fully_occupied = sum(self._thread_ready_status) == 0
                        if self._fully_occupied:
                            self._start_listen_fut = asyncio.Future()
                    if message['data'] == self._close_token:
                        return
                    if self._closing:
                        return
            except Exception as e:
                if debug:
                    raise e


    async def _worker_thread(self, thread_idx: int):
        loop = asyncio.get_running_loop()
        while True:
            info = await self._worker_thread_futs[thread_idx]
            self._logger.debug(f"Pid: {self.pid}, thread no.{thread_idx} waked up，task info: {info}.")
            if info is False: return # Receive False to close this subthread
            while True:
                # After being activated, it transitions from sleep state to working state. Loop until the task queue is empty.
                try:
                    if self._allowed_type_limit is None:
                        task_bytes: Optional[bytes] = await self.redis.lpop(self._query_queue_name)
                    else:
                        task_bytes: Optional[bytes] = await self._queue_script((self._query_queue_name, ))

                    if task_bytes is None: break
                    task_level, task_route, task_uuid, form_data = self._query_string_parser(task_bytes)
                    self._logger.info(f"Pid: {self.pid}, new task: {task_level}, '{task_route}', {task_uuid}")
                    result_channel_name = f"{self._result_channel_prefix}{task_uuid}"
                    await self.redis.publish(result_channel_name, self._confirm_token)
                    self._logger.debug(f"Pid: {self.pid}, `{result_channel_name}` confirmed")
                    method = self._method_map[task_route]
                    if method.__name__[0] == 'a':
                        res = await method(self.redis, self.mysql, **form_data)
                    else:
                        res = await loop.run_in_executor(self._executor, partial(method, self.redis, self.mysql, **form_data))
                    res = json.dumps(res)
                    self._logger.info(f"Pid: {self.pid}, return: {res[:20]}")
                    await self.redis.publish(result_channel_name, res)

                except Exception as e:
                    if debug:
                        raise e
            
            self._logger.debug(f"Pid: {self.pid}, thread no.{thread_idx}, end of task cycle.")
            self._worker_thread_futs[thread_idx] = asyncio.Future()
            self._thread_ready_status[thread_idx] = 1
            self._fully_occupied = False
            if not self._start_listen_fut.done():
                self._start_listen_fut.set_result(None)
            if self._closing:
                self._worker_thread_futs[thread_idx].set_result(False)
                return


    async def run_serve(self):
        if self._run_startup:
            await self.async_startup()
        self._logger.info(f"Startup comlete, pid: {self.pid}.")
        pubsub = self.redis.pubsub() 
        self._start_listen_fut = asyncio.Future()
        self._start_listen_fut.set_result(None)
        self._thread_ready_status = [1 for _ in range(self._thread_num)]
        for thread_idx in range(self._thread_num):
            self._worker_thread_futs.append(asyncio.Future())
            self._worker_thread_full_list.append(asyncio.create_task(self._worker_thread(thread_idx)))

        if self._has_synchronou_process:
            executor = ThreadPoolExecutor
        else:
            executor = PseudoThreadPoolExecutor

        with executor(max_workers = maximum_subthread) as myexecutor:
            if myexecutor:
                myexecutor.set_daemon_opts(min_workers = 2)
            self._executor = myexecutor
            try:
                async with pubsub as pschannel:
                    await pschannel.subscribe(self._inbound_query_channel)
                    await self._waiting_new_query(pschannel)
                    await pschannel.unsubscribe(self._inbound_query_channel)
            finally:
                await pubsub.close()
                self._close_flag_lst['main'] = 1
                if sum(self._close_flag_lst.values()) ==3:
                    self._close_flag.set_result(None)
        await self.wait_closed()


    async def close(self):
        await self.redis.publish(self._inbound_query_channel, self._close_token.decode())
        self._close_flag_lst['close'] = 1
        if sum(self._close_flag_lst.values()) ==3:
            self._close_flag.set_result(None)


    async def wait_closed(self):
        await self._close_flag


    async def _loop_trigger(self):
        '''
        Activate a program that has been hanged up so that it can receive the shutdown signal.
        '''
        while True:
            if self._closing:
                break
            await asyncio.sleep(3)
        self._close_flag_lst['ltrigger'] = 1
        if sum(self._close_flag_lst.values()) ==3:
            self._close_flag.set_result(None)

    def graceful_shutdown(self, sig: int, frame):
        self._logger.info(f"Pid: {self.pid}, catch sig: {sig}")
        self._closing = True
        loop = asyncio.get_running_loop()
        loop.create_task(self.close())