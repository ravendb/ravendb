using System.Collections.Generic;

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

        public int OperationVersion { get; set; }

        public static readonly Dictionary<OperationTypes, int> TcpVersions = new Dictionary<OperationTypes, int>
        {
            {OperationTypes.Cluster, 10},
            {OperationTypes.Heartbeats, 20},
            {OperationTypes.Replication, 30},
            {OperationTypes.Subscription, 40}
        };
    }
}
