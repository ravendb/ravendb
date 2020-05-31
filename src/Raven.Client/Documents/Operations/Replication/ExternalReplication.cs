using System;
using Raven.Client.Documents.Replication;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    public class ExternalReplication : ExternalReplicationBase
    {
        public ExternalReplication() { }

        public ExternalReplication(string database, string connectionStringName) : base(database, connectionStringName)
        {
            
        }

        public TimeSpan DelayReplicationFor;

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(DelayReplicationFor)] = DelayReplicationFor;
            return json;
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
