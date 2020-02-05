namespace Tests.ResourceSnapshotAggregator
{
    public class ServiceSettings
    {
        public int NotificationListenerPort { get; set; }
        public InfluxDBSettings InfluxDB { get; set; }
        public JenkinsSettings Jenkins { get; set; }
    }

    public class InfluxDBSettings
    {
        public string Url { get; set; }
        public string Token { get; set; }
    }

    public class JenkinsSettings
    {
        public string Url { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string ApiKey { get; set; }
    }
}
