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
            {OperationTypes.Cluster, 1},
            {OperationTypes.Heartbeats, 1},
            {OperationTypes.Replication, 1},
            {OperationTypes.Subscription, 1}
        };
    }
}
