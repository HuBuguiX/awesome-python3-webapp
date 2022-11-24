import asyncio, logging, aiomysql; logging.basicConfig(level=logging.INFO)

#创建日志函数
def log(sql, args=()):
    logging.info(sql.replace('?', '%s'), args or ())

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
    # 从连接池获取一个链接
    with (await __pool) as conn:
        cur = await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            # fetchmany 可以获取行数为 size 的多行查询结果集，返回一个列表
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
            # 获取函数影响的行数
            affected = cur.rowcount
            await cur.close()
        except BaseException as _:
            raise
        return affected

# 2022/11/21
# 创建指定数量的sql语句占位符
def create_args_string(num):
    L = []
    for _ in range(num):
        L.append('?')
    return ', '.join(L)

# 定义字段与类之间的映射
class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s: %s>' % (self.__clas__.__name__, self.column_type, self.name)

class stringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)

class BooleanField(Field):
    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)

class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'real', primary_key, default)

class TextField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)

class ModelMetaclass(type):
    # cls: 当前准备创建的类的对象
    # name: 类的名字
    # bases: 类继承的父类集合
    # attrs: 类的方法集合
    def __new__(cls, name, bases, attrs):
        # 排除 Model 类本身，返回它自己
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        # 尝试从类属性 '__table__' 中获取表名，若不存在该属性，则将类名视为表名
        tableName = attrs.get('__table__', None) or name
        logging.info('Found model: %s (table: %s)' % (name, tableName))
        # 从表类中提取类属性和其关联的字段类对象
        mappings = dict() # 存储映射关系
        fields = [] # 存储非主键字段
        primaryKey = None
        # attrs.items 存储了类属性与字段类对象的映射关系
        # k: 类属性名称
        # v: 字段类的对象
        for k, v in attrs.items():
            # 判断类属性关联的对象是否是字段对象
            if isinstance(v, Field):
                logging.info('Found mapping: %s ==> %s' % (k, v))
                # 将类属性与字段类对象的映射关系保存
                mappings[k] = v
                # 查找主键
                if v.primary_key: # 如果字段类对象的 primary_key 属性为 True，则为主键
                    if primaryKey: # 如果 primaryKey 有值，则主键已存在。抛出“重复主键错误”
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    # 否则，主键还不存在，应保存主键
                    primaryKey = k
                # 否则，将普通字段保存到 fields
                else:
                    fields.append(k)
        if not primaryKey:
            # 抛出“主键不存在”错误
            raise RuntimeError('Primary key not found.')
        # 删除与字段对象关联的类属性。实例属性会遮盖类同名属性，容易造成运行时错误
        for k in mappings.keys():
            attrs.pop(k)
        # 将非主键字段用反单引号引起来
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        # 将先前保存的类属性与字段类对象的映射关系 mappings 保存到类属性 '__mappings__'
        attrs['__mappings__'] = mappings
        # 将表名保存到类属性的 '__table__' 中
        attrs['__table__'] = tableName
        # 将主键保存到类属性的 '__primary__key__' 中
        attrs['__primary_key_'] = primaryKey
        # 将非主键字段保存到类属性的 '__fields__' 中
        attrs['__fields__'] = fields
        # 构造默认的 SELECT, INSERT, UPDATE和 DELETE 语句，将其保存到对应的类属性中
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)

class Model(dict, metaclass=ModelMetaclass):
    def __init__(self, **kw):
        super().__init__(**kw)

    # 通过属性访问键值对
    def __gerattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no arrtribute '%s'" % key)
    
    # 通过属性设置键值对
    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    # 访问表类的实例属性，若某属性无值，则从表类与表的映射关系中查找默认值
    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            # 如果 Value 为 None，则从表类的类属性与字段类对象的映射关系中查找默认值
            field = self.__mappings__[key] # 获取实例属性对应的字段类对象
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value