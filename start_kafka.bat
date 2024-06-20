@echo off

REM Bước 1: Mở cmd và khởi động Zookeeper
echo Starting Zookeeper...
start cmd /k "cd C:\Kafka && .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties"

REM Đợi Zookeeper khởi động
timeout /t 10

REM Bước 2: Mở cmd mới và khởi động Kafka Server
echo Starting Kafka Server...
start cmd /k "cd C:\Kafka && .\bin\windows\kafka-server-start.bat .\config\server.properties"

REM Đợi Kafka Server khởi động
timeout /t 10

REM Bước 3: Mở cmd mới và tạo topic, khởi động Kafka Connect
echo Creating topic and starting Kafka Connect...
start cmd /k "cd C:\Kafka\bin\windows && kafka-topics --create --topic stock_topic --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092 && cd C:\Kafka && bin\windows\connect-distributed.bat config\connect-distributed.properties"

REM Đợi Kafka Connect khởi động
timeout /t 10

REM Bước 4: Mở cmd mới và gửi cấu hình connector tới Kafka Connect, kiểm tra trạng thái
echo Posting connector configuration and checking status...
start cmd /k "cd C:\Kafka\config && curl -X POST -H \"Content-Type: application/json\" --data @postgres-sink.json http://localhost:8083/connectors && curl -X GET http://localhost:8083/connectors/postgres-sink-connector/status"

REM Bước 5: Mở cmd mới và khởi động Kafka Console Producer
echo Starting Kafka Console Producer...
start cmd /k "cd C:\Kafka\bin\windows && kafka-console-producer.bat --broker-list localhost:9092 --topic stock_topic"

echo Kafka và các thành phần đã được khởi động thành công.
pause
