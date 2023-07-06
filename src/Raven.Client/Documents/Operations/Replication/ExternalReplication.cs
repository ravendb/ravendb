using System;
using Raven.Client.Documents.Replication;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    public class ExternalReplication : ExternalReplicationBase, IExternalReplication, ITombstoneDeletionBlocker
    {
        public ExternalReplication()
        {
        }

        public ExternalReplication(string database, string connectionStringName) : base(database, connectionStringName)
        {
        }

        public TimeSpan DelayReplicationFor { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(DelayReplicationFor)] = DelayReplicationFor;
            return json;
        }

        public override DynamicJsonValue ToAuditJson()
        {
            return ToJson();
        }
        public override bool IsEqualTo(ReplicationNode other)
        {
            if (other is ExternalReplication externalReplication)
            {
                return base.IsEqualTo(other) &&
                       DelayReplicationFor == externalReplication.DelayReplicationFor;
            }

            return false;
        }
    }
}
