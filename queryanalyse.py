# coding=UTF-8
import getopt
import re
import os
import sys

usage = ''' usage: python [script's path] [option]
        ALL options need to assign:
        -path : 读取日志文件和写入日志文件的路径 必须有
        -f : 读取文件的名字 必须有 binlog 日志文件
        -of : 写入文件 满足条件分析后的数据  如果没有默认名字 读取文件名.log
        -td : 数据库的名称
        -tt : 表的名称
        Example: python queryanalyse.py -td=activity -tt=activity  -path=mysqllog/ -f=mysql-bin.000268 -of=268123.txt 
        '''

class queryanalyse:
    def __init__(self):
        #初始化
        self.readname = ''
        self.outname = ''
        self.path = ''
        self.ofpath = ''
        self.logpath = ''
        self.search_td =''
        self.search_tb =''
        self.total_num = 0

        self._get_db()

        if self.path.endswith('/') == False:
            self.path = self.path + '/'

        if self.path == '' or self.readname == '': 
            print(usage)
            sys.exit()

        self.fpath = self.path  + self.readname
        if os.path.exists(self.fpath) == False:
            print '*****************************************************\n'
            print('ERROR ：文件不存在 '  + self.fpath + '\n')
            print '*****************************************************\n'
            sys.exit()

        self.logpath = self.path + '/tmp.log'

        # 初始化输出路径
        if self.outname == '':
            self.ofpath = self.path  + self.readname + '.log'
        else:
            self.ofpath = self.path  + self.outname

    def _get_db(self):
        #解析用户输入的选项参数值
        if len(sys.argv) == 1:
            print(usage)
            sys.exit(1)
        elif sys.argv[1] == '--help':
            print(usage)
            sys.exit()
        elif len(sys.argv) > 2:
            for i in sys.argv[1:]:
                _argv = i.split('=')
                if _argv[0] == '-f':
                    self.readname = _argv[1]
                elif _argv[0] == '-of':
                    self.outname = _argv[1]
                elif _argv[0] == '-path':
                    self.path = _argv[1]
                elif _argv[0] == '-td':
                    self.search_td = _argv[1]
                elif _argv[0] == '-tt':
                    self.search_tb = _argv[1]
                else:
                    print(usage)

    def rowrecord(self):
        record_sql = ''
        start_print = ''

        isSave = False
        isEffective = False
        out_sql = ''
        isEnd = False

        with open(self.logpath,'r') as binlog_file ,open( self.ofpath, 'wt') as out_file:

            for bline in binlog_file:

                if bline.find('Table_map:') != -1:

                    l = bline.index('server')
                    n = bline.index('Table_map')
                    begin_time = bline[:l:].rstrip(' ').replace('#', '20')
                    begin_time = begin_time[0:4] + '-' + begin_time[4:6] + '-' + begin_time[6:]
                    db_name = bline[n::].split(' ')[1].replace('`', '').split('.')[0]
                    tb_name = bline[n::].split(' ')[1].replace('`', '').split('.')[1]
                    # print 'start time:' + begin_time
                    isEffective = False
                    isEnd = False
                    out_sql = '\nstart time:' + begin_time + '\n'
                    out_sql = out_sql + 'Database.Table: ' + db_name + '.' + tb_name + '\n'

                elif bline.startswith('### INSERT INTO'):
                    # print bline.replace("\n", "")
                    isEffective = True
                    out_sql = out_sql + bline

                elif bline.startswith('### UPDATE'):
                    # print bline.replace("\n", "")
                    isEffective = True
                    out_sql = out_sql + bline

                elif bline.startswith('### UPDATE'):
                    # print bline.replace("\n", "")
                    isEffective = True
                    out_sql = out_sql + bline

                elif bline.startswith('### DELETE FROM'):
                    # print bline.replace("\n", "")
                    isEffective = True
                    out_sql = out_sql + bline

                elif bline.find('Xid =') != -1:
                    isEffective = False
                    isEnd = True
                    l = bline.index('server')
                    end_time = bline[:l:].rstrip(' ').replace('#', '20')
                    end_time = end_time[0:4] + '-' + end_time[4:6] + '-' + end_time[6:]
                    # print 'end time:' + end_time
                
                elif bline.startswith('# at') == False and isEffective:
                    # print bline.replace("\n", "")
                    out_sql = out_sql + bline

                if isEnd and ((self.search_tb == '' and self.search_td == '') or (self.search_td != '' and db_name == self.search_td) or (self.search_tb != '' and tb_name == self.search_tb)):
                    out_file.write(out_sql)
                    self.total_num = self.total_num + 1
                    # print out_sql
                    out_sql = ''

    def mysqlbinlog(self):
        os.system("mysqlbinlog -v --base64-output=DECODE-ROWS " + self.fpath + " > " + self.logpath)

    def clearfile(self):
        os.system("rm -f " +  self.logpath)

    def totalnum(self):
        return bytes(self.total_num)

    def outpath(self):
        return self.ofpath


def main():
    print '\n*******************************************\n'
    print '************  初始化数据  *****************\n'
    query = queryanalyse()
    print '******  开始处理binlog日志信息  ***********\n'
    query.mysqlbinlog()
    print '********  开始分析并输出文件  **************\n'
    query.rowrecord()
    print '***********  清理无用文件  ****************\n'
    query.clearfile()
    print '***********  数据分析完成  ****************\n'
    print '*******************************************\n'
    print '\n******************************************************************************'
    print '\n**** 总条数为 ' + query.totalnum()
    print '\n**** 输出路径 ' + query.outpath()
    print '\n******************************************************************************'

if __name__ == "__main__":
    main()
