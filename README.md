# Moyo

Moyo is extended Apache JENA Fuseki 2.4.1 for cyber physical applications.

[日本語](#ja)

## Extensions

Moyo can mix changing data of the outer world into RDF graph.

* Realtime data mixing.
<BR>You can specify 'proxy nodes' as data ports to outer world.
<BR>Moyo replaces proxy node IRIs to associated
real world values on a Sparql query variable assignments.
* WebSocket connections.
 * You can put proxy node values via a WebSocket connection.
 * You can get proxy node value changes via a WebSocket connection.

Some ohter extensions are under construction.

Moyo is compatible with Fuseki unless RDF includes special IRI for Moyo.

## Installation

The installation procedure is same as [Fuseki](https://jena.apache.org/documentation/serving_data/).

Moyo sample data instruction is [here](moyo-doc/en/installation.md).

## Manual

[Manual](moyo-doc/en/manual.md)

----
<a name="#ja"></a>
# Moyoについて

MoyoはApache JENA Fusekiへサイバーフィジカルシステム向けの拡張機能を追加したものです。

## 拡張機能


拡張機能のためのIRIを含むRDFデータを使わなければ、Fusekiとほぼ互換に動作します。

## 実装済み

実装済みの拡張機能はつぎのとおりです。

 * 外部データ連携
 <BR>RDFグラフにプロキシノードと呼ぶノードを定義し、それを介して外部システムとリアルタイムに連携します。アクセスが集中しなければ、RaspberryPI程度のハードウエアで1ms未満で応答できます。
  * プロキシノードへ値を書き込むと、外部へその値を送信する。
  * プロキシノードの値を読み出すと、外部から受信した現在値を返す。
 * WebSocket接続
 <BR> 
  * SPARQLクエリを実行する。
  * 値が変化したときにプロキシノードの値をクライアントへ送信する。
  * 外部からプロキシノードの現在値を書き換える。
 * プロキシノードへのHTTPによるアクセス
  * SPARQLとは異なるURLでプロキシノード

## 実装検討中

今後実装を検討している拡張機能はつぎのとおりです。

 * 認証・認可
 <BR>RDFに認可を制御するデータを付加すると、認証ユーザごとに見えるデータを
 制限できるようにします。
 <BR>WebSocketとタグパスへのアクセスパスは、Fusekiの標準パスとは異なっています。
 このパスについては、現状ではApache Shiroによる制御ができません。

## インストールと実行

MoyoはJava8ランタイム上で動作します。<BR>
インストール・実行方法はFusekiと全く同じです。
解凍した配布パッケージを実行すると、
localhost:3030ポートでWebサーバが立ち上がります。
ここへWebブラウザで接続してデモデータを実行します。

* [インストールとサンプルの実行](moyo-doc/ja/install.md)
* [拡張機能](moyo-doc/ja/extension.md)
* [課題](moyo-doc/ja/issue.md)

## 実装意図

Moyoは、数十年以上にわたってサービスを続ける分散システムを実現する
ことを目的に開発しました。

* [実装意図](moyo-doc/ja/purpose.md)
* [空間OS](moyo-doc/ja/fieldos.md)


このサイトの運用設計がおいついてませんが、こんなのではじめましょうか。

* 一般ユーザはここに参加「勉強会チーム」に参加
* 全員が自動的に「オフトピック」「タウンスクウェア」チャネルへ参加
 * タウンスクウェア： 公式連絡、教えてくれ、教えてやる。
 * オフトピック： なんでも。愚痴、雄たけびなども可
