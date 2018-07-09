using System;
using System.Collections.Generic;

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
            Ping,
            TestConnection
        }

        public string DatabaseName { get; set; }

        public string SourceNodeTag { get; set; }

        public OperationTypes Operation { get; set; }

        public int OperationVersion { get; set; }

        public string Info { get; set; }

        public static readonly int NumberOfRetriesForSendingTcpHeader = 2;
        public static readonly int ClusterTcpVersion = 10;
        public static readonly int HeartbeatsTcpVersion = 20;
        public static readonly int ReplicationTcpVersion = 33;
        public static readonly int SubscriptionTcpVersion = 40;
        public static readonly int TestConnectionTcpVersion = 50;

        public static bool OperationVersionSupported(OperationTypes operationType, int version)
        {
            if (_operationsToSupportedProtocolVersions.TryGetValue(operationType, out var supportedProtocols) == false)
                return false;
            return supportedProtocols.Contains(version);
        }

        private static readonly Dictionary<OperationTypes,HashSet<int>> _operationsToSupportedProtocolVersions
        = new Dictionary<OperationTypes, HashSet<int>>
            {
                [OperationTypes.Ping] = new HashSet<int> { -1},
                [OperationTypes.None] = new HashSet<int> { -1 },
                [OperationTypes.Drop] = new HashSet<int> { -2 },
                [OperationTypes.Subscription] = new HashSet<int> { 40 },
                [OperationTypes.Replication] = new HashSet<int> { 31, 33 },
                [OperationTypes.Cluster] = new HashSet<int> { 10 },
                [OperationTypes.Heartbeats] = new HashSet<int> { 20 },
                [OperationTypes.TestConnection] = new HashSet<int> { 50 },
        };
        public static int GetOperationTcpVersion(OperationTypes operationType)
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
                    return ReplicationTcpVersion;
                case OperationTypes.Cluster:
                    return  ClusterTcpVersion;
                case OperationTypes.Heartbeats:
                    return  HeartbeatsTcpVersion;
                case OperationTypes.TestConnection:
                    return  TestConnectionTcpVersion;
                default:
                    throw new ArgumentOutOfRangeException(nameof(operationType), operationType, null);
            }
        }
    }
}
