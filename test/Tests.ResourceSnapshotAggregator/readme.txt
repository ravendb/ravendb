//setup influxdb container
docker network create influxdb
docker run -d --name influxdb -p 8083:8083 -p 8086:8086 --expose 8090 --expose 8099 --net=influxdb influxdb:latest

//start influxdb container
docker start influxdb

//connect to influx container (to configure it, for example)
docker exec -it influxdb bash

//setup influxdb ui (chronograf)
docker run -d --name=chronograf -p 8888:8888 --net=influxdb --restart=always chronograf --influxdb-url=http://influxdb:8086

//start influxdb ui container
docker start chronograf
(by default login at http://localhost:8888/)

//setup grafana (to connect to influxdb and set up graphs)
docker pull grafana/grafana
docker run -d --name grafana --net=influxdb -p 1948:3000 grafana/grafana
(by default login at http://localhost:1948 -> username, password = admin/admin)

//start grafana container
docker start grafana
