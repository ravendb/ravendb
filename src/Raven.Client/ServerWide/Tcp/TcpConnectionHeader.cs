using System;
using System.Collections.Generic;
using System.Linq;

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

        public static readonly int PingBaseLine = -1;
        public static readonly int NoneBaseLine = -1;
        public static readonly int DropBaseLine = -2;
        public static readonly int ClusterBaseLine = 10;
        public static readonly int HeartbeatsBaseLine = 20;
        public static readonly int Heartbeats41200 = 41_200;
        public static readonly int ReplicationBaseLine = 31;
        public static readonly int ReplicationAttachmentMissing = 40_300;
        public static readonly int ReplicationAttachmentMissingVersion41 = 41_300;
        public static readonly int SubscriptionBaseLine = 40;
        public static readonly int SubscriptionIncludes = 41_400;
        public static readonly int TestConnectionBaseLine = 50;

        public static readonly int ClusterTcpVersion = ClusterBaseLine;
        public static readonly int HeartbeatsTcpVersion = Heartbeats41200;
        public static readonly int ReplicationTcpVersion = ReplicationAttachmentMissingVersion41;
        public static readonly int SubscriptionTcpVersion = SubscriptionIncludes;
        public static readonly int TestConnectionTcpVersion = TestConnectionBaseLine;

        static TcpConnectionHeaderMessage()
        {
            // validate
            var operations = new[]
            {
                OperationTypes.Cluster,
                OperationTypes.Drop,
                OperationTypes.Heartbeats,
                OperationTypes.None,
                OperationTypes.Ping,
                OperationTypes.Replication,
                OperationTypes.Subscription,
                OperationTypes.TestConnection
            };
            foreach (var operation in operations)
            {
                var versions = OperationsToSupportedProtocolVersions[operation];
                var features = SupportedFeaturesByProtocol[operation];
                if (features.Keys.SequenceEqual(versions) == false)
                {
                    throw new ArgumentException();
                }

                var version = versions[0];

                switch (operation)
                {
                    case OperationTypes.None:
                        if (version != NoneBaseLine)
                            throw new ArgumentException();
                        if (features[NoneBaseLine] == null)
                            throw new ArgumentException();
                        break;
                    case OperationTypes.Drop:
                        if (version != DropBaseLine)
                            throw new ArgumentException();
                        if (features[DropBaseLine] == null)
                            throw new ArgumentException();
                        break;
                    case OperationTypes.Subscription:
                        if (version != SubscriptionTcpVersion)
                            throw new ArgumentException();
                        if (features[SubscriptionTcpVersion] == null)
                            throw new ArgumentException();
                        break;
                    case OperationTypes.Replication:
                        if (version != ReplicationTcpVersion)
                            throw new ArgumentException();
                        if (features[ReplicationTcpVersion] == null)
                            throw new ArgumentException();
                        break;
                    case OperationTypes.Cluster:
                        if (version != ClusterTcpVersion)
                            throw new ArgumentException();
                        if (features[ClusterTcpVersion] == null)
                            throw new ArgumentException();
                        break;
                    case OperationTypes.Heartbeats:
                        if (version != HeartbeatsTcpVersion)
                            throw new ArgumentException();
                        if (features[HeartbeatsTcpVersion] == null)
                            throw new ArgumentException();
                        break;
                    case OperationTypes.Ping:
                        if (version != PingBaseLine)
                            throw new ArgumentException();
                        if (features[PingBaseLine] == null)
                            throw new ArgumentException();
                        break;
                    case OperationTypes.TestConnection:
                        if (version != TestConnectionTcpVersion)
                            throw new ArgumentException();
                        if (features[TestConnectionTcpVersion] == null)
                            throw new ArgumentException();
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
        }

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
                public bool BaseLine = true;
            }
            public class NoneFeatures
            {
                public bool BaseLine = true;
            }
            public class DropFeatures
            {
                public bool BaseLine = true;
            }
            public class SubscriptionFeatures
            {
                public bool BaseLine = true;

                public bool Includes;
            }
            public class ClusterFeatures
            {
                public bool BaseLine = true;
            }
            public class HeartbeatsFeatures
            {
                public bool BaseLine = true;
                public bool SendChangesOnly;
            }
            public class TestConnectionFeatures
            {
                public bool BaseLine = true;
            }
            public class ReplicationFeatures
            {
                public bool BaseLine = true;
                public bool MissingAttachments;
                public bool Counters;
                public bool ClusterTransaction;
            }
        }

        private static readonly Dictionary<OperationTypes, List<int>> OperationsToSupportedProtocolVersions
            = new Dictionary<OperationTypes, List<int>>
            {
                [OperationTypes.Ping] = new List<int>
                {
                    PingBaseLine
                },
                [OperationTypes.None] = new List<int>
                {
                    NoneBaseLine
                },
                [OperationTypes.Drop] = new List<int>
                {
                    DropBaseLine
                },
                [OperationTypes.Subscription] = new List<int>
                {
                    SubscriptionIncludes,
                    SubscriptionBaseLine
                },
                [OperationTypes.Replication] = new List<int>
                {
                    ReplicationAttachmentMissingVersion41,
                    ReplicationAttachmentMissing,
                    ReplicationBaseLine
                },
                [OperationTypes.Cluster] = new List<int>
                {
                    ClusterBaseLine
                },
                [OperationTypes.Heartbeats] = new List<int>
                {
                    Heartbeats41200,
                    HeartbeatsBaseLine
                },
                [OperationTypes.TestConnection] = new List<int>
                {
                    TestConnectionBaseLine
                }
            };

        private static readonly Dictionary<OperationTypes, Dictionary<int, SupportedFeatures>> SupportedFeaturesByProtocol
            = new Dictionary<OperationTypes, Dictionary<int, SupportedFeatures>>
            {
                [OperationTypes.Ping] = new Dictionary<int, SupportedFeatures>
                {
                    [PingBaseLine] = new SupportedFeatures(PingBaseLine)
                    {
                        Ping = new SupportedFeatures.PingFeatures()
                    }
                },
                [OperationTypes.None] = new Dictionary<int, SupportedFeatures>
                {
                    [NoneBaseLine] = new SupportedFeatures(NoneBaseLine)
                    {
                        None = new SupportedFeatures.NoneFeatures()
                    }
                },
                [OperationTypes.Drop] = new Dictionary<int, SupportedFeatures>
                {
                    [DropBaseLine] = new SupportedFeatures(DropBaseLine)
                    {
                        Drop = new SupportedFeatures.DropFeatures()
                    }
                },
                [OperationTypes.Subscription] = new Dictionary<int, SupportedFeatures>
                {
                    [SubscriptionIncludes] = new SupportedFeatures(SubscriptionIncludes)
                    {
                        Subscription = new SupportedFeatures.SubscriptionFeatures
                        {
                            Includes = true
                        }
                    },
                    [SubscriptionBaseLine] = new SupportedFeatures(SubscriptionBaseLine)
                    {
                        Subscription = new SupportedFeatures.SubscriptionFeatures()
                    }
                },
                [OperationTypes.Replication] = new Dictionary<int, SupportedFeatures>
                {
                    [ReplicationAttachmentMissingVersion41] = new SupportedFeatures(ReplicationAttachmentMissingVersion41)
                    {
                        Replication = new SupportedFeatures.ReplicationFeatures
                        {
                            Counters = true,
                            ClusterTransaction = true,
                            MissingAttachments = true
                        }
                    },
                    /*While counter is a newer feature 'ReplicationAttachmentMissing' is a newer release and we must check the version by the order of the release*/
                    [ReplicationAttachmentMissing] = new SupportedFeatures(ReplicationAttachmentMissing)
                    {
                        Replication = new SupportedFeatures.ReplicationFeatures
                        {
                            MissingAttachments = true
                        }
                    },
                    [ReplicationBaseLine] = new SupportedFeatures(ReplicationBaseLine)
                    {
                        Replication = new SupportedFeatures.ReplicationFeatures()
                    }
                },
                [OperationTypes.Cluster] = new Dictionary<int, SupportedFeatures>
                {
                    [ClusterBaseLine] = new SupportedFeatures(ClusterBaseLine)
                    {
                        Cluster = new SupportedFeatures.ClusterFeatures()
                    }
                },
                [OperationTypes.Heartbeats] = new Dictionary<int, SupportedFeatures>
                {
                    [Heartbeats41200] = new SupportedFeatures(Heartbeats41200)
                    {
                        Heartbeats = new SupportedFeatures.HeartbeatsFeatures
                        {
                            SendChangesOnly = true
                        }
                    },
                    [HeartbeatsBaseLine] = new SupportedFeatures(HeartbeatsBaseLine)
                    {
                        Heartbeats = new SupportedFeatures.HeartbeatsFeatures()
                    }
                },
                [OperationTypes.TestConnection] = new Dictionary<int, SupportedFeatures>
                {
                    [TestConnectionBaseLine] = new SupportedFeatures(TestConnectionBaseLine)
                    {
                        TestConnection = new SupportedFeatures.TestConnectionFeatures()
                    }
                }
            };


        public enum SupportedStatus
        {
            OutOfRange,
            NotSupported,
            Supported
        }

        public static SupportedStatus OperationVersionSupported(OperationTypes operationType, int version, out int current)
        {
            current = -1;
            if (OperationsToSupportedProtocolVersions.TryGetValue(operationType, out var supportedProtocols) == false)
            {
                throw new ArgumentException($"This is a bug. Probably you forgot to add '{operationType}' operation " +
                                            $"to the '{nameof(OperationsToSupportedProtocolVersions)}' dictionary.");
            }

            for (var i = 0; i < supportedProtocols.Count; i++)
            {
                current = supportedProtocols[i];
                if (current == version)
                    return SupportedStatus.Supported;

                if (current < version)
                    return SupportedStatus.NotSupported;
            }

            return SupportedStatus.OutOfRange;
        }

        public static int GetOperationTcpVersion(OperationTypes operationType, int index = 0)
        {
            // we don't check the if the index go out of range, since this is expected and means that we don't have 
            switch (operationType)
            {
                case OperationTypes.Ping:
                case OperationTypes.None:
                    return -1;
                case OperationTypes.Drop:
                    return -2;
                case OperationTypes.Subscription:
                case OperationTypes.Replication:
                case OperationTypes.Cluster:
                case OperationTypes.Heartbeats:
                case OperationTypes.TestConnection:
                    return OperationsToSupportedProtocolVersions[operationType][index];
                default:
                    throw new ArgumentOutOfRangeException(nameof(operationType), operationType, null);
            }
        }

        public static SupportedFeatures GetSupportedFeaturesFor(OperationTypes type, int protocolVersion)
        {
            if (SupportedFeaturesByProtocol[type].TryGetValue(protocolVersion, out var features) == false)
                throw new ArgumentException($"{type} in protocol {protocolVersion} was not found in the features set.");
            return features;
        }
    }
}
