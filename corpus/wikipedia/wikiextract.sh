#!/usr/bin/env bash
# Wikipedia のダンプファイル [lang]wiki-[YYYYMMDD]-pages-articles.xml.bz2 から
# Wiki マークアップを除外し記事をテキストに変換します。
# Docker が使用できる場合はこのコマンドで実行可能:
#
# docker run -it -v "$PWD:/opt/data" --name wikiextract --rm python /bin/bash /opt/data/wikiextract.sh
#
cd /opt/data

file="$1"
if [ ! -f "$file" ]
then
  if [[ $file =~ [0-9]{8} || "$file" = "latest" ]]
  then
    file=jawiki-$file-pages-articles.xml.bz2
  elif [ "$file" = "" ]
  then
    file=jawiki-latest-pages-articles.xml.bz2
  fi
fi

if [[ "$file" =~ ^([a-z]{2})wiki-(.+)-pages-articles\.xml\.bz2$ ]]
then
  ln=${BASH_REMATCH[1]}
  id=${BASH_REMATCH[2]}
else
  echo "ERROR: invalid filename: $file"
  exit 1
fi

# Wikipedia のダンプファイルをダウンロード
if [ ! -f "$file" ]
then
  echo "Loading Wikipedia Data..."
  wget https://dumps.wikimedia.org/${ln}wiki/$id/$file
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

outdir=${ln}wiki-$id-pages-articles
echo "exitracting wikipedia: $outdir"
python wikiextractor/WikiExtractor.py -o $outdir $file

# まだ文書ごとに <doc>～</doc> で囲まれているので combine.scala で1行1文書に変換などを行う
