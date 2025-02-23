# TimescaleDB
## Findings & Benchmarks

### C++ drivers
![Screenshot 2025-02-23 at 12 14 47 PM](https://github.com/user-attachments/assets/6a787539-af44-49b5-82f9-a33ad608fa5c)
 - OZO might be a good choice as it supports high concurrency, async processing and connection pooling, but comes with a hard learning curve because of Boost.asio.
 - taoPQ and pgfe might also be good alternatives due to performance and ease of use.

### Setup
* installing postgres
>> sudo apt-get install postgresql
>> sudo apt install gnupg postgresql-common apt-transport-https lsb-release wget
>> sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh
>> sudo apt install postgresql-server-dev-17

* installing TSdb
>> echo "deb https://packagecloud.io/timescale/timescaledb/ubuntu/ $(lsb_release -c -s) main" | sudo tee /etc/apt/sources.list.d/timescaledb.list
>> sudo apt update
>> sudo apt install timescaledb-2-postgresql-17 postgresql-client-17
>> sudo timescaledb-tune
>> sudo systemctl restart postgresql
>> sudo -u postgres psql
>> \password postgres

* install OZO
>> sudo apt update
>> sudo apt install build-essential cmake libboost-all-dev
>> git clone https://github.com/yandex/ozo.git
>> cd ozo
>> mkdir build && cd build
>> cmake .. -DOZO_BUILD_TESTS=OFF -DBOOST_ROOT=/opt/boost_1_83_0
>> make
>> sudo make install
>> #define BOOST_HANA_CONFIG_ENABLE_STRING_UDL

* create file
>> nano timescale_ozo.cpp

* compile
>> g++ -std=c++17 timescale_ozo.cpp -o timescale_ozo -I/usr/local/include -I/usr/include/postgresql -L/usr/local/lib -lpq -lboost_system -pthread

* run
>> ./timescale_ozo

### Datasets:
 - Bitcoin Historical Dataset ([Kaggle](https://www.kaggle.com/datasets/prasoonkottarathil/btcinusd?select=BTC-2021min.csv))
 - Crypto Currency Dataset ([Kaggle](https://www.kaggle.com/datasets/tr1gg3rtrash/time-series-top-100-crypto-currency-dataset))
 - Bitcoin Transactions ([Harvard Dataverse](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/ZLBYTZ))

