from influxdb_client import InfluxDBClient, WriteOptions


class Influxdb_Components:
    __client = None
    __write_api = None
    __bucket = None
    __org = None

    def __init__(self, url, token, org, bucket):
        if 'localhost' in url:
            url = 'http://localhost:8086'
        self.__client = InfluxDBClient(url=url, token=token, org=org)
        self.__write_api = self.__client.write_api(write_options=WriteOptions(batch_size=1))
        self.__bucket = bucket
        self.__org = org

    def get_client(self):
        return self.__client

    def get_results_by_run(self, measurement_name, run_id):
        """ :return: Query for getting recorded results by run_id from db """
        query = f'from(bucket: "{self.__bucket}") |> range(start: -30d) |> filter(fn: (r) => r._measurement == "{measurement_name}" and r.run == "{run_id}")'
        print(self.__client.url)
        return self.__client.query_api().query(query, org=self.__org)

    def delete_results_by_run(self, measurement_name, run_id):
        """ :return: Query for deleting recorded results by run_id from db """
        # InfluxDB v2 does not support DELETE queries directly. You would need to use the API to delete data.
        raise NotImplementedError("InfluxDB v2 does not support DELETE queries directly. Use the API to delete data.")

    def write_points(self, points):
        # Sending points to db
        self.__write_api.write(bucket=self.__bucket, org=self.__org, record=points)
