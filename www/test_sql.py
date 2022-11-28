import orm
import asyncio
from models import User, Blog, Comment

async def test(loop):
    await orm.create_pool(loop=loop, user='root', password='password', db='awesome')
    u = User(name='测试', email='test2@qq.com', passwd='12345678', image='about:blank')
    await u.save()
    orm.__pool.close()
    await orm.__pool.wait_closed()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test(loop))
    loop.close()