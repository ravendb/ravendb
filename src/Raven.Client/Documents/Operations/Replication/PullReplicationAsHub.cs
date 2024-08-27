using System;
using Raven.Client.Documents.Replication;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    internal sealed class PullReplicationAsHub : ExternalReplication
    {
        public PullReplicationMode Mode = PullReplicationMode.HubToSink;

        public PullReplicationAsHub()
        {
        }

        public PullReplicationAsHub(string database, string connectionStringName) : base(database, connectionStringName)
        {
        }

        public override ReplicationType GetReplicationType() => ReplicationType.PullAsHub;

        public override bool IsEqualTo(ReplicationNode other)
        {
            if (other is PullReplicationAsHub hub)
            {
                return base.IsEqualTo(other) &&
                       string.Equals(Url, hub.Url, StringComparison.OrdinalIgnoreCase) &&
                       string.Equals(Name, hub.Name, StringComparison.OrdinalIgnoreCase);
            }

            return false;
        }

        public override ulong GetTaskKey()
        {
            var hashCode = base.GetTaskKey();
            hashCode = (hashCode * 397) ^ CalculateStringHash(Url);
            return (hashCode * 397) ^ (ulong)Mode;
        }

        public override DynamicJsonValue ToJson()
        {
            var djv = base.ToJson();
            djv[nameof(Mode)] = Mode;
            return djv;
        }

        public override DynamicJsonValue ToAuditJson()
        {
            var djv = base.ToAuditJson();
            djv[nameof(Mode)] = Mode;
            return djv;
        }

        public override string ToString()
        {
            return $"Replication Hub to {FromString()}. " +
                   $"Hub Task Name: '{Name}', " +
                   $"Mode: '{Mode}'";
        }

        public override string GetDefaultTaskName()
        {
            return ToString();
        }
    }
}
