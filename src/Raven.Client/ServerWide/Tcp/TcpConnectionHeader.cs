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
        
        public static readonly int PingBaseLine4000 = -1;
        public static readonly int NoneBaseLine4000 = -1;
        public static readonly int DropBaseLine4000 = -2;
        public static readonly int ClusterBaseLine4000 = 10;
        public static readonly int HeartbeatsBaseLine4000 = 20;
        public static readonly int ReplicationBaseLine4000 = 31;
        public static readonly int ReplicationAttachmentMissing = 33;
        public static readonly int SubscriptionBaseLine4000 = 40;
        public static readonly int TestConnectionBaseLine4000 = 50;

        public static readonly int ClusterTcpVersion = ClusterBaseLine4000;
        public static readonly int HeartbeatsTcpVersion = HeartbeatsBaseLine4000;
        public static readonly int ReplicationTcpVersion = ReplicationAttachmentMissing;
        public static readonly int SubscriptionTcpVersion = SubscriptionBaseLine4000;
        public static readonly int TestConnectionTcpVersion = TestConnectionBaseLine4000;

        public class SupportedFeatures
        {            
            public readonly int ProtocolVersion;

            public SupportedFeatures(int version)
            {
                ProtocolVersion = version;
            }

            public PingFeatures Ping { get; set; }           
            public NoneFeatures None { get; set; }
            public DropFeatures Drop { get; set; }
            public SubscriptionFeatures Subscription { get; set; }
            public ClusterFeatures Cluster { get; set; }
            public HeartbeatsFeatures Heartbeats { get; set; }
            public TestConnectionFeatures TestConnection { get; set; }

            public ReplicationFeatures Replication { get; set; }

            public class PingFeatures
            {
                public bool BaseLine4000 = true;
            }
            public class NoneFeatures
            {
                public bool BaseLine4000 = true;
            }
            public class DropFeatures
            {
                public bool BaseLine4000 = true;
            }
            public class SubscriptionFeatures
            {
                public bool BaseLine4000 = true;
            }
            public class ClusterFeatures
            {
                public bool BaseLine4000 = true;
            }

            public class HeartbeatsFeatures
            {
                public bool BaseLine4000 = true;
            }
            public class TestConnectionFeatures
            {
                public bool BaseLine4000 = true;
            }
            public class ReplicationFeatures
            {
                public bool BaseLine4000 = true, MissingAttachments;
            }
        }

        /// <summary>
        /// This dictionary maps (operation type, protocol version) to a list of supported features, supported features comes with a protocol versions
        /// and the list of supported features is sorted by the most up to date protocol meaning OperationsToSupportedProtocolVersions[(type,version)][0]
        /// will have the same version as the key.version and it is the most up to date version that is supported for key.version.
        /// This dictionary has dual purpose:
        /// 1. map available features for given protocol type and version.
        /// 2. allow to find a prev version that the current version support and map its features.
        ///
        /// * When inserting new entries to this dictionary you must place them as the first item in the list of supported features
        /// Lets say we had
        /// OperationsToSupportedProtocolVersions[('Foo',5)] = new List<SupportedFeatures> {SupportedFeatures5}
        /// and we want to add a new protocol '6' it should look like this:
        /// OperationsToSupportedProtocolVersions[('Foo',6)] = new List<SupportedFeatures> {SupportedFeatures6, SupportedFeatures5}
        /// </summary>
        private static readonly Dictionary<(OperationTypes,int), List<SupportedFeatures>> OperationsToSupportedProtocolVersions
            = new Dictionary<(OperationTypes, int), List<SupportedFeatures>>
            {
                [(OperationTypes.Ping, PingBaseLine4000)] = 
                    new List<SupportedFeatures>
                    {
                        new SupportedFeatures(PingBaseLine4000){Ping = new SupportedFeatures.PingFeatures()}
                    },
                [(OperationTypes.None, NoneBaseLine4000)] = 
                    new List<SupportedFeatures>
                    {
                        new SupportedFeatures(NoneBaseLine4000){None = new SupportedFeatures.NoneFeatures()}
                    },
                [(OperationTypes.Drop, DropBaseLine4000)] = 
                    new List<SupportedFeatures>
                    {
                        new SupportedFeatures(DropBaseLine4000) { Drop = new SupportedFeatures.DropFeatures() }
                    },
                [(OperationTypes.Subscription, SubscriptionBaseLine4000)] = 
                    new List<SupportedFeatures>
                    {
                        new SupportedFeatures(SubscriptionBaseLine4000){Subscription = new SupportedFeatures.SubscriptionFeatures()}
                    },
                [(OperationTypes.Replication, ReplicationAttachmentMissing)] = 
                    new List<SupportedFeatures>
                    {
                        new SupportedFeatures(ReplicationAttachmentMissing){Replication = new SupportedFeatures.ReplicationFeatures{MissingAttachments = true}},
                        new SupportedFeatures(ReplicationBaseLine4000){Replication = new SupportedFeatures.ReplicationFeatures()}
                    },
                [(OperationTypes.Replication, ReplicationBaseLine4000)] =
                    new List<SupportedFeatures>
                    {
                        new SupportedFeatures(ReplicationBaseLine4000){Replication = new SupportedFeatures.ReplicationFeatures()}
                    },
                [(OperationTypes.Cluster, ClusterBaseLine4000)] =
                    new List<SupportedFeatures>
                    {
                        new SupportedFeatures(ClusterBaseLine4000) {Cluster = new SupportedFeatures.ClusterFeatures()}
                    },
                [(OperationTypes.Heartbeats, HeartbeatsBaseLine4000)] =
                    new List<SupportedFeatures>
                    {
                        new SupportedFeatures(HeartbeatsBaseLine4000) { Cluster = new SupportedFeatures.ClusterFeatures()}
                    },
                [(OperationTypes.TestConnection, TestConnectionBaseLine4000)] =
                    new List<SupportedFeatures>
                    {
                        new SupportedFeatures(TestConnectionBaseLine4000) { TestConnection = new SupportedFeatures.TestConnectionFeatures()}
                    },
            };
        public static (bool Supported, int PrevSupported) OperationVersionSupported(OperationTypes operationType, int version)
        {
            var prev = -1;
            if (OperationsToSupportedProtocolVersions.TryGetValue((operationType, version), out var supportedProtocols) == false)
                return (false, prev);
            
            for (var i =0; i< supportedProtocols.Count; prev = supportedProtocols[i].ProtocolVersion, i++)
            {
                var current = supportedProtocols[i].ProtocolVersion;
                if (current == version)
                    return (true, prev);

                if (current > version)
                    return (false, prev);
            }

            return (false, prev);
        }


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

        public static SupportedFeatures GetSupportedFeaturesFor(OperationTypes type, int optionsProtocolVersion)
        {
            return OperationsToSupportedProtocolVersions[(type, optionsProtocolVersion)][0];
        }
    }
}
