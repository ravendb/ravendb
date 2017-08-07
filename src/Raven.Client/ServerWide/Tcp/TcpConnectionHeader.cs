namespace Raven.Client.ServerWide.Tcp
{
    public class TcpConnectionHeaderMessage
    {
        public enum OperationTypes
        {
            None,
            Subscription,
            Replication,
            Cluster,
            Heartbeats
        }

        public string DatabaseName { get; set; }

        public string SourceNodeTag { get; set; }

        public OperationTypes Operation { get; set; }
    }
}