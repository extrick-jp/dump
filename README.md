MySQLデータベースのダンプをとるプログラムです。
Webサーバーとデータベースサーバーが別々になっていて、Webサーバーにmysqldump/maridadb_dumpがインストールされていない環境で、MySQLデータベースのバックアップをとる必要があってこのプログラムを書きました。
初めてのRustプログラムです。おおいに笑ってやってください。

dump -u DB_USER [-p DB_PASS] [-h DB_HOST] DB_NAME

DB_HOST: 省略すると localhost が指定されます。

DB_PASS: パスワードなしで開ける場合は省略してください。
