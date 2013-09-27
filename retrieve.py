#------coding:utf-8------

import re
import os
import sys
import shutil
import logging
import sqlite3
import urllib
import urllib2
import unicodedata
from optparse import OptionParser
from bs4 import BeautifulSoup
from spider import Crawler

cid_map = dict()	# url map to cid
cat_map = dict()	# cid map to category

def Cat_for_p(p, cat):
	try:
		cites = p.find_all('sup', attrs = {'class': 'reference'})
		for cite in cites:
			cid = int(cite.find('a', href = True).attrs['href'].split('-')[-1])
			if cid not in cat_map:
				cat_map[cid] = set()
			cat_map[cid].add(cat)
	except Exception,e:
		print e

def Categorize(dbname, cur, path):
	cur.execute('select url, html, picUrl from ' + dbname + ' where id == 1')
	wiki_website = cur.fetchone()
	wiki_url = wiki_website[0]
	try:
		headers = {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}
		req = urllib2.Request(url = wiki_url, headers = headers)
		html = urllib2.urlopen(url = req, data = None, timeout = 5).read()
	except Exception,e:
		print e
		return None
	print 'construct the map between url and cid...'
	if wiki_url.endswith('/'):
		wiki_url = wiki_url[:-1]
	ref_block = Crawler.findRefBlock(html)
	refs = ref_block.find_all('li')
	for ref in refs:
		cid = int(ref.attrs['id'].split('-')[-1])
		ref_tag = ref.find('a', attrs = {'href' : re.compile('^http|^/'), 'rel' : 'nofollow', 'class' : 'external text'})
		if ref_tag:
			ref_url = ref_tag['href']
			if ref_url.startswith('/'):
				ref_url = wiki_url + ref_url
			cid_map[ref_url] = cid
	print 'construct the map between cid and category...'
	text_block = Crawler.findmainbody(BeautifulSoup(html))
	cat = 'Abstract'
	if not os.path.exists(path + cat):
		os.mkdir(path + cat)
	for tag in text_block.find('p').next_siblings:
		if tag.name == 'h2':
			cat = tag.find('span').get_text()
			if not os.path.exists(path + cat):
				os.mkdir(path + cat)
		elif tag.name == 'p':
			Cat_for_p(tag, cat)

def RetrieveData(dbname, path):
	conn = sqlite3.connect('data.sql')
	cur = conn.cursor()
	headers = {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}

	if not os.path.exists(path + 'doc'):
		os.mkdir(path + 'doc')
	if not os.path.exists(path + 'img'):
		os.mkdir(path + 'img')
	Categorize(dbname, cur, path)

	cur.execute('select url, html, picUrl from ' + dbname)
	website = cur.fetchone()
	cnt = 1
	while website:
		# 按照url为每个网站的信息建立一个文件夹
		# dirname = str(cnt);
		# if not os.path.exists(path + dirname):
		# 	os.mkdir(path + dirname)
		# 将网站的文本信息存入doc/cnt.txt
		url = website[0]
		print '\n\n>>processing ' + url
		if url.endswith('/'):
			url = url[:-1]
		doc = open(path + 'doc/' + str(cnt) + '.txt', 'w')
		print >> doc, unicodedata.normalize('NFKD',url).encode('ascii','ignore') + '\n\n'
		print >> doc, unicodedata.normalize('NFKD',website[1]).encode('ascii','ignore')
		doc.close()
		# 获取picUrl相对应的图片并保存
		if cnt != 1 and not ((website[0] in cid_map) and (cid_map[website[0]] in cat_map)):
			print 'pass image retrieval process.'
		else:
			picUrls = website[2].split(',')
			for picUrl in picUrls:
				if picUrl == '' or not (picUrl.endswith('.png') or picUrl.endswith('.jpg') or picUrl.endswith('.jpeg')):
					continue
				# 尝试规范url格式
				if picUrl.startswith('//'):
					picUrl = 'http:' + picUrl
				elif not picUrl.startswith('http'):
					if picUrl.startswith('/'):
						picUrl = url + picUrl
					else:
						picUrl = url + '/' + picUrl
				dest_path = path + 'img/' + picUrl.split('/')[-1]
				try:		
					req = urllib2.Request(url = picUrl, headers = headers)
					imghandler = urllib2.urlopen(url = req, data = None, timeout = 10)
					f = open(dest_path, 'wb')
					f.write(imghandler.read())
					f.close()
					print '--succeed in retrieving ' + picUrl
					# move image to corresponding category
					for cat_dir in cat_map[cid_map[website[0]]]:
						shutil.copy(dest_path, path + cat_dir)
					if cat_map[cid_map[website[0]]]:
						os.remove(dest_path)
				except KeyboardInterrupt:
					print '>>skip current image.'
				except Exception, e:
					logging.error("%s %s", e, picUrl)
				finally:
					# if os.path.exists(dest_path):
					# 	os.remove(dest_path)
					f.close()
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
		if not os.path.exists('db/' + options.dbname):
			os.mkdir('db/' + options.dbname)
		path = 'db/' + options.dbname + '/'
		logging.basicConfig(filename = 'retrieve.log',level = 3)
		RetrieveData(options.dbname, path)