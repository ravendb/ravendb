using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication;
using Sparrow.Json;

namespace Raven.Server.Documents.Replication
{
    public static class ReplicationHelper
    {
        public static ReplicationNode GetReplicationNodeByType(BlittableJsonReaderObject bjro)
        {
            if (bjro == null || bjro.TryGet("Type", out ReplicationNode.ReplicationType type) == false)
                return null;
            
            return type switch
            {
                ReplicationNode.ReplicationType.PullAsSink => DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<PullReplicationAsSink>(bjro,
                    "PullReplicationAsSink"),
                ReplicationNode.ReplicationType.External => DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<ExternalReplication>(bjro,
                    "ExternalReplication"),
                ReplicationNode.ReplicationType.Internal => DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<InternalReplication>(bjro,
                    "InternalReplication"),
                ReplicationNode.ReplicationType.Migration => DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<BucketMigrationReplication>(bjro,
                    "BucketMigrationReplication"),
                _ => throw new ArgumentOutOfRangeException()
            };
        }
    }
}
