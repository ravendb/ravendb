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
            if (bjro == null || bjro.TryGet(nameof(ReplicationNode.Type), out ReplicationNode.ReplicationType type) == false)
                return null;
            
            return type switch
            {
                ReplicationNode.ReplicationType.PullAsHub => DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<PullReplicationAsHub>(bjro,
                    "PullReplicationAsHub"),
                ReplicationNode.ReplicationType.PullAsSink => DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<PullReplicationAsSink>(bjro,
                    "PullReplicationAsSink"),
                ReplicationNode.ReplicationType.External => DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<ExternalReplication>(bjro,
                    "ExternalReplication"),
                ReplicationNode.ReplicationType.Internal => DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<InternalReplication>(bjro,
                    "InternalReplication"),
                ReplicationNode.ReplicationType.Migration => DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<BucketMigrationReplication>(bjro,
                    "BucketMigrationReplication"),
                _ => throw new ArgumentOutOfRangeException()
            };
        }
    }
}
