#!/usr/bin/env bash

# docker -it -v "$PWD:/opt/data" python /bin/bash

file=jawiki-latest-pages-articles.xml.bz2

# Wikipedia 日本語最新版のダンプをダウンロード
if [ ! -f $file ]
then
  echo "Loading Wikipedia Data..."
  wget https://dumps.wikimedia.org/jawiki/latest/$file
fi

# WikiExtractor でテキストファイルに変換
if [ ! -d "wikiextractor" ]
then
  # https://github.com/attardi/wikiextractor
  git clone https://github.com/attardi/wikiextractor.git
  cd wikiextractor
  python setup.py install
  cd ..
fi

python wikiextractor/WikiExtractor.py -o wikipwdia $file

# まだ文書ごとに <doc>～</doc> で囲まれているので combine.scala で1行1文書に変換する