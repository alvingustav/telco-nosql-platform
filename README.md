# Telco NoSQL Platform

Platform analisis data telekomunikasi menggunakan Cassandra (DB1) dan MongoDB (DB2) dengan query aggregator untuk tugas besar ROBD.

## Fitur Utama

- **Dual NoSQL Database**: Cassandra untuk CDR data, MongoDB untuk customer data
- **Query Aggregator**: Menggabungkan query dari kedua database
- **Performance Comparison**: Analisis performa dengan dan tanpa index
- **Web Interface**: Dashboard interaktif untuk monitoring dan analisis
- **Real-time Updates**: WebSocket untuk update status real-time

## Instalasi

### 1. Clone Repository
git clone https://github.com/alvingustav/telco-nosql-platform

cd telco-nosql-platform

### 2. Install Dependencies
pip install -r Requirements.txt


### 3. Setup Databases
#### Cassandra
wget https://downloads.apache.org/cassandra/4.1.3/apache-cassandra-4.1.3-bin.tar.gz

tar -xzf apache-cassandra-4.1.3-bin.tar.gz

cd apache-cassandra-4.1.3

bin/cassandra -f

#### MongoDB
sudo apt-get install -y mongodb

sudo systemctl start mongod

sudo systemctl enable mongod

### 4. Load Data
python scripts/setup_databases.py
python scripts/load_existing_data.py --data-dir telco_data_export

### 5. Run Platform
python src/web_app/app.py


Akses platform di: http://localhost:5000

## Struktur Data

### Cassandra (DB1) - CDR Data
- **call_records**: Data panggilan
- **sms_records**: Data SMS  
- **data_usage**: Data penggunaan internet

### MongoDB (DB2) - Customer Data
- **customers**: Profil pelanggan
- **subscriptions**: Data langganan
- **billing**: Data tagihan
- **customer_support**: Tiket support

## Query Types

1. **DB1 Only**: Analisis volume panggilan dari Cassandra
2. **DB2 Only**: Segmentasi pelanggan dari MongoDB  
3. **Combined**: Gabungan behavior analysis dari kedua DB

## Performance Testing

Platform menyediakan fitur perbandingan performa:
- Query tanpa index
- Query dengan index
- Analisis improvement percentage

## API Endpoints

- `POST /api/setup-databases` - Setup database schemas
- `POST /api/load-existing-data` - Load data dari JSON files
- `POST /api/execute-query` - Execute queries
- `POST /api/performance-test` - Run performance comparison
- `POST /api/create-indexes` - Create database indexes

## Kontribusi

1. Fork repository
2. Create feature branch
3. Commit changes
4. Push to branch
5. Create Pull Request

## License

MIT License

