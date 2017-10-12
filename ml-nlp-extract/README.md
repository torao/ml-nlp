# データ抽出モジュール

ML-NLP 用にデータを抽出する処理を実装するサブプロジェクト。

```
$ sbt "runMain at.hazm.ml.nlp.extract.wikipedia.MakeCategoryIndex jawiki-20170801-pages-articles.xml.bz2
```

## テーブル構成

Extract によって抽出される Wikipedia 記事のテーブル。

### ARTICLES

記事のタイトルと内容を保持する。

| カラム名 | 型 | 説明 |
|:---------|:---|:-----|
| ID | INTEGER | MediaWiki 上のページ ID |
| TITLE | TEXT | タイトル |
| CONTENT | TEXT | 内容 |

### PAGES

ページの ID、タイトル、リダイレクト先を保持する。このテーブルには `ARTICLES` テーブルに保存されている記事が全て含まれる他、内容を持たないタイトルのみのリダイレクト元ページ、`Wikipedia:` ページや `Category:` ページなど記事以外のページも含まれる。

タイトルが `CATEGORY:` から始まるかでカテゴリページの ID を抽出することができる。

| カラム名 | 型 | 説明 |
|:---------|:---|:-----|
| ID | INTEGER | MediaWiki 上のページ ID |
| TITLE | TEXT | 検索用に正規化されたタイトル |
| REDIRECT_TO | TEXT | リダイレクトが設定されている場合は正規化されたタイトル |

### CATEGORIES 

ページに設定された個々のカテゴリを表す。ID は MediaWiki ではなく抽出時に採番したもの。

| カラム名 | 型 | 説明 |
|:---------|:---|:-----|
| ID | INTEGER | カテゴリ ID |
| TITLE | TEXT | 正規化されたタイトル |

### PAGE_CATEGORIES

ページとそのページが所属するカテゴリの関連を示す。

| カラム名 | 型 | 説明 |
|:---------|:---|:-----|
| PAGE_ID | INTEGER | MediaWiki 上のページ ID |
| CATEGORY_ID | INTEGER | カテゴリ ID |

### CORPUS

形態素を ID にマッピングしたもの。

| カラム名 | 型 | 説明 |
|:---------|:---|:-----|
| ID | INTEGER | 形態素 ID (0～) |
| MORPH | TEXT | 形態素 |

### ARTICLE_MORPHS

形態素解析された記事。

| カラム名 | 型 | 説明 |
|:---------|:---|:-----|
| ID | INTEGER | MediaWiki 上のページ ID |
| MORPHS | TEXT | 形態素解析した本文の形態素 ID 空白区切り |


