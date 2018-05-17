using System;

namespace Raven.Client.ServerWide.Tcp
{
    public class TcpConnectionHeaderMessage
    {
        public enum OperationTypes
        {
            None,
            Drop,
            Subscription,
            Replication,
            Cluster,
            Heartbeats,
            Ping
        }

        public string DatabaseName { get; set; }

        public string SourceNodeTag { get; set; }

        public OperationTypes Operation { get; set; }

        public int OperationVersion { get; set; }

        public string Info { get; set; }

        public const int NumberOfRetriesForSendingTcpHeader = 2;
        public const int ClusterTcpVersion = 10;
        public const int HeartbeatsTcpVersion = 20;
        public const int ReplicationTcpVersion = 32;

        public class Legacy
        {
            public const int V40ReplicationTcpVersion= 31;
        }


        public const int SubscriptionTcpVersion = 40;

        public static int GetOperationTcpVersion(OperationTypes operationType, int remoteVersion = 0)
        {
            switch (operationType)
            {
                case OperationTypes.Ping:
                case OperationTypes.None:
                    return -1;
                case OperationTypes.Drop:
                    return -2;
                case OperationTypes.Subscription:
                    return SubscriptionTcpVersion;
                case OperationTypes.Replication:
                    if (Legacy.V40ReplicationTcpVersion == remoteVersion)
                        return remoteVersion;
                    return ReplicationTcpVersion;
                case OperationTypes.Cluster:
                    return  ClusterTcpVersion;
                case OperationTypes.Heartbeats:
                    return  HeartbeatsTcpVersion;
                default:
                    throw new ArgumentOutOfRangeException(nameof(operationType), operationType, null);
            }
        }
    }
}
