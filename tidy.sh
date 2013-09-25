#!/bin/bash

tidy_for_topic()
{
	if [ ! -e 'img' ]; then
		mkdir img
	fi
	if [ ! -e 'doc' ]; then
		mkdir doc
	fi
	
	for ref in `ls`; do
		if [ -d $ref ] && [ $ref != 'img' ] && [ $ref != 'doc' ]; then
			cd $ref
			for file in `ls | grep "\.\(jpg\|jpeg\|png\)"`; do
				mv $file ../img/
			done
			if [ -e 'doc.txt' ]; then
				mv doc.txt ../doc/${ref}.txt
			fi
			cd ..
			rm -r $ref
		fi
	done
}

for i in `ls`; do
	if [ -d $i ]; then
		cd $i
		tidy_for_topic
		cd ..
	fi
done
