namespace Raven.Client.Server.Tcp
{
    public class TcpConnectionHeaderMessage
    {
        public enum OperationTypes
        {
            None,
            BulkInsert,
            Subscription,
            Replication,
            TopologyDiscovery,
            Cluster
        }

        public string DatabaseName { get; set; }

        public OperationTypes Operation { get; set; }

        public string AuthorizationToken { get; set; }
    }
}