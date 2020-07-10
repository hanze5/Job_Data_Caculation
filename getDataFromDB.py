from pymysql import connect, cursors

host = '47.113.123.159'
port = 3306
db  = 'job_info'
user = 'pa'
passwd = '258258cqu'
charset='utf8'


conn = connect(host=host, port=port, db=db,user=user, passwd=passwd, charset=charset)
cur = conn.cursor(cursor=cursors.DictCursor)

# sql = "select Jname from 51job,58job,cnzp,lagoujob,liepin"
sql_51 = "select Jname ,Jtype from 51job"
sql_58 = "select Jname ,Jtype from 58job"
sql_cnzp = "select Jname ,Jtype from cnzp"
sql_liepin = "select Jname ,Jtype from liepin"
sql_lagou = "select Jname ,Jtype from lagoujob"





cur.execute(sql_51)
dict_51 = cur.fetchall()


cur.execute(sql_58)
dict_58 = cur.fetchall()

cur.execute(sql_cnzp)
dict_cnzp = cur.fetchall()

cur.execute(sql_liepin)
dict_liepin = cur.fetchall()

cur.execute(sql_lagou)
dict_lagou = cur.fetchall()

dictall = dict_51+dict_58+dict_cnzp+dict_liepin+dict_lagou

class wuhu:
    dataList = dictall



