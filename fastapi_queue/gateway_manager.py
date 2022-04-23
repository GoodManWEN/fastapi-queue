import aioredis
import asyncio
import uuid
import msgpack as json
from async_timeout import timeout

debug = False

class DistributedTaskApplyManager:

    _scriper = [None, ]

    def __new__(cls, *args, **kwargs):
        instance = super().__new__(cls)
        instance._scriper = cls._scriper
        return instance

    def __init__(
        self, 
        redis: aioredis.client, 
        request_path: str, 
        confirm_timeout: int = 5, 
        process_timeout: int = 10
    ):
        self.redis = redis 
        self._request_path = request_path
        self._token_name = f'routelimit:{request_path}'
        self._idf_uuid = uuid.uuid4()
        self._result_channel_name = f"channel:result:{self._idf_uuid}"
        self._confirm_timeout = confirm_timeout
        self._process_timeout = process_timeout
        self._success: bool = False
        self._confirm_token = b'confirmed'
        self._channel_to_publish_query = "channel:query"
        self._queue_of_query = "queue:web"
        self._occupancy_token_name = f"occupied:{self._request_path}:{self._idf_uuid}"
        self.rclt = self.redis_channel_listening_result

    def success(self):
        return self._success

    async def redis_channel_listening_result(self, channel: aioredis.client.PubSub, *args, **kwargs):
        asyncio.create_task(self._publish_query(*args, **kwargs))
        task_status_activate = False 
        channel = self._ps_channel
        try:
            # Ensure that a worker claims the task within 5 seconds.
            async with timeout(self._confirm_timeout):
                while True:
                    message = await channel.get_message(ignore_subscribe_messages=True, timeout=10)
                    if message is not None:
                        if message['data'] == self._confirm_token:
                            task_status_activate = True
                            break
        except asyncio.TimeoutError:
            asyncio.create_task(self._withdraw_query(*args, **kwargs))
        except Exception as e:
            if debug:
                raise e

        if not task_status_activate:
            return False, -1

        try:
            async with timeout(self._process_timeout):
                while True:
                    message = await channel.get_message(ignore_subscribe_messages=True, timeout = self._process_timeout + 5)
                    if message is not None:
                        result = json.loads(message['data'])
                        break
        except asyncio.TimeoutError:
            return False, -2
        except Exception as e:
            if debug:
                raise e
        else:
            return True, result 

    async def _publish_query(self, form_data: dict = {}, task_level: int = 0):
        '''
        Adding new tasks to the queue
        '''
        async with self.redis.pipeline(transaction=False) as pipe:
            content_str = f'{task_level}:{self._request_path}:{self._idf_uuid}'
            res = await pipe.rpush(self._queue_of_query, content_str.encode() + b':' + json.dumps(form_data)).publish(self._channel_to_publish_query, b'0').execute()

    async def _withdraw_query(self, form_data: dict = {}, task_level: int = 0):
        '''
        Undo tasks that have been added to the queue if they have not been confirmed
        '''
        content_str = f'{task_level}:{self._request_path}:{self._idf_uuid}'
        res = await self.redis.lrem(self._queue_of_query, 1, content_str.encode() + b':' + json.dumps(form_data))


    async def  __aenter__(self):
        script = self._scriper[0]
        if not script:
            script = self.redis.register_script("if tonumber(redis.call('get', KEYS[1])) > 0 then redis.call('decr', KEYS[1]);redis.call('set', KEYS[2], 'nx', 'ex', ARGV[1]);return 1 else return 0 end")
        success: int = await script(keys = (self._token_name, self._occupancy_token_name), args = (self._process_timeout + self._confirm_timeout,))
        success: bool = bool(success)
        self._success = success
        if success:
            self._pubsub = redis.pubsub()
            entered = False
            try:
                self._ps_channel = await self._pubsub.__aenter__()
                entered = True
                await self._ps_channel.subscribe(self._result_channel_name)
            except:
                if entered:
                    await self._pubsub.__aexit__(None, None, None)
                self._success = False
        return self

    async def __aexit__(self, type, value, trace):
        if self._success:
            try:
                await self._ps_channel.subscribe(self._result_channel_name)
            finally:
                await self._pubsub.__aexit__(None, None, None)
            async with self.redis.pipeline(transaction=False) as pipe:
                await pipe.incr(self._token_name).delete(self._occupancy_token_name).execute()
            await self._pubsub.close()
        return False