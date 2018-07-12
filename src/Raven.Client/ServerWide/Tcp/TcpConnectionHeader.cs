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
        
        public static readonly int PingBaseLine40 = -1;
        public static readonly int NoneBaseLine40 = -1;
        public static readonly int DropBaseLine40 = -2;
        public static readonly int ClusterBaseLine40 = 10;
        public static readonly int HeartbeatsBaseLine40 = 20;
        public static readonly int ReplicationBaseLine40 = 31;
        public static readonly int ReplicationAttachmentMissing = 33;
        public static readonly int SubscriptionBaseLine40 = 40;
        public static readonly int TestConnectionBaseLine40 = 50;

        public static readonly int ClusterTcpVersion = ClusterBaseLine40;
        public static readonly int HeartbeatsTcpVersion = HeartbeatsBaseLine40;
        public static readonly int ReplicationTcpVersion = ReplicationAttachmentMissing;
        public static readonly int SubscriptionTcpVersion = SubscriptionBaseLine40;
        public static readonly int TestConnectionTcpVersion = TestConnectionBaseLine40;
        public static (bool Supported, int PrevSupported) OperationVersionSupported(OperationTypes operationType, int version)
        {
            var prev = -1;
            if (OperationsToSupportedProtocolVersions.TryGetValue(operationType, out var supportedProtocols) == false)
                return (false, prev);
            
            for (var i =0; i< supportedProtocols.Count; prev = supportedProtocols[i], i++)
            {
                var current = supportedProtocols[i];
                if (current == version)
                    return (true, prev);

                if (current > version)
                    return (false, prev);
            }

            return (false, prev);
        }

        private static readonly Dictionary<OperationTypes, List<int>> OperationsToSupportedProtocolVersions
        = new Dictionary<OperationTypes, List<int>>
            {
                [OperationTypes.Ping] = new List<int> { PingBaseLine40 },
                [OperationTypes.None] = new List<int> { NoneBaseLine40 },
                [OperationTypes.Drop] = new List<int> { DropBaseLine40 },
                [OperationTypes.Subscription] = new List<int> { SubscriptionBaseLine40 },
                [OperationTypes.Replication] = new List<int> { ReplicationAttachmentMissing, ReplicationBaseLine40 },
                [OperationTypes.Cluster] = new List<int> { ClusterBaseLine40 },
                [OperationTypes.Heartbeats] = new List<int> { HeartbeatsBaseLine40 },
                [OperationTypes.TestConnection] = new List<int> { TestConnectionBaseLine40 },
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
