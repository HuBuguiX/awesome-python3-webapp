import asyncio, logging, aiomysql; logging.basicConfig(level=logging.INFO)

#创建日志函数
def log(sql, args=()):
    logging.info('SQL: %s' % sql)

#创建连接池函数
async def create_pool(loop, **kw):
    # **kw 可变关键字参数。传参格式为：key=value。kw在函数内部为一个dict
    logging.info('create database connection pool...')
    # 声明全局变量。连接池由全局变量__pool存储，缺省情况下将编码设置为utf8，自动提交事务
    global __pool
    __pool = await aiomysql.create_pool(
        host = kw.get('host', 'localhost'),
        port = kw.get('port', 3306),
        user = kw['user'],
        password = kw['password'],
        db=kw['db'],
        charset = kw.get('charset', 'utf8'),
        autocommit = kw.get('autocommit', True),
        maxsize = kw.get('maxsize', 10),
        minsize = kw.get('minsize', 1),
        loop = loop
    )

async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    with (await __pool) as conn:
        cur = await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            # fetchmany 可以获取行数为 size 的多行查询结果集， 返回一个列表
            rs = await cur.fetchmany(size)
        else:
            # fetchall 可以获取一个查询结果的所有行，返回一个列表
            rs = await cur.fetchall()
        await cur.close()
        # 输出查询结果行数
        logging.info('rows returned: %s' % len(rs))
        return rs

async def execute(sql, args):
    log(sql, args)
    global __pool
    with (await __pool) as conn:
        try:
            cur = await conn.cursor()
            await cur.execute(sql.replace('?', '%s'), args)
            # 后取函数影响的函数
            affected = cur.rowcount
            await cur.close()
        except BaseException as _:
            raise
        return affected