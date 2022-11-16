import logging; logging.basicConfig(level=logging.INFO)
import asyncio, os, json, time
from datetime import datetime
from aiohttp import web

def index(request):
    return web.Response(body=b'<h1>Awesome</h1>', content_type='text/html', headers={'content_type': 'text/html'})

#url分发器
def setup_routes(app):
    #对域名和函数进行链接
    app.router.add_get('/', index)
    #输出信息
    logging.info('server started at http://127.0.0.1:8080...')

#创建webapp对象
app = web.Application()
setup_routes(app)
#执行webapp，设定域名、ip、端口
web.run_app(app, host='127.0.0.1', port=8080)
