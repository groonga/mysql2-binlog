# mysql2-replication

## Description

mysql2-replication is an extension of [mysql2 gem](https://rubygems.org/gems/mysql2). It adds support for replication client feature based on `libmariadb.so` that is a MySQL/MariaDB client library provided by MariaDB.

## Install

```bash
gem install mysql2-replication
```

## Usage

```ruby
require "mysql2-replication"

client = Mysql2::Client.new(username: "root",
                            password: "secret")
# Get the latest binlog file and position from source.
# You can specify them manually.
master_status = client.query("SHOW MASTER STATUS").first
file = master_status["File"]
position = master_status["Position"]

replication_client = Mysql2Replication::Client(client)
replication_client.file_name = file
replication_client.start_position = position
replication_client.open do
  replication_client.each do |event|
    pp event
  end
end
```

## License

The MIT license. See `LICENSE.txt` for details.
