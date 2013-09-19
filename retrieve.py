#------coding:utf-8------

import os
import sys
import logging
import sqlite3
import urllib
import urllib2
import unicodedata
from optparse import OptionParser

def RetrieveData(dbname, path):
	conn = sqlite3.connect('data.sql')
	cur = conn.cursor()
	headers = {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}

	cur.execute('select url, html, picUrl from ' + dbname)
	website = cur.fetchone()
	cnt = 1
	while website:
		# 按照url为每个网站的信息建立一个文件夹
		dirname = str(cnt);
		if not os.path.exists(path + dirname):
			os.mkdir(path + dirname)
		# 将网站的文本信息存入doc.txt
		url = website[0]
		print '\n\n>>processing ' + url
		if url.endswith('/'):
			url = url[:-1]
		doc = open(path + dirname + '/doc.txt', 'w')
		print >> doc, unicodedata.normalize('NFKD',url).encode('ascii','ignore') + '\n\n'
		print >> doc, unicodedata.normalize('NFKD',website[1]).encode('ascii','ignore')
		doc.close()
		# 获取picUrl相对应的图片并保存
		picUrls = website[2].split(',')
		for picUrl in picUrls:
			if picUrl == '':
				continue
			# 尝试规范url格式
			if picUrl.startswith('//'):
				picUrl = 'http:' + picUrl
			elif not picUrl.startswith('http'):
				if picUrl.startswith('/'):
					picUrl = url + picUrl
				else:
					picUrl = url + '/' + picUrl
			try:		
				req = urllib2.Request(url = picUrl, headers = headers)
				urllib2.urlopen(req)
				urllib.urlretrieve(picUrl, path + dirname + '/' + picUrl.split('/')[-1])
				print '--succeed in retrieving ' + picUrl
			except Exception, e:
				logging.error("%s %s", e, picUrl)
		website = cur.fetchone()
		cnt += 1

	conn.close()

if __name__ == '__main__':
	optParser = OptionParser(usage = '%prog --dbname [dbname]')
	optParser.add_option('--dbname', dest = 'dbname', type = 'string')
	(options, args) = optParser.parse_args()
	if len(sys.argv) < 3:
		print optParser.print_help()
	else:
		if not os.path.exists(options.dbname):
			os.mkdir(options.dbname)
		path = options.dbname + '/'
		logging.basicConfig(filename = 'retrieve.log',level = 3)
		RetrieveData(options.dbname, path)