#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging; logging.basicConfig(level=logging.INFO)
import asyncio, os, json, time
from datetime import datetime
from aiohttp import web

async def index(request):
    return web.Response(body='<h1>Awesome</h1>', content_type='text/html')

def init():
    app = web.Application()
    app.add_routes([web.get('/', index)])
    logging.info('Server started at 127.0.0.1:8080')
    web.run_app(app, host='127.0.0.1', port=8080)

if __name__ == '__main__':
    init()
