using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Replication;
using Raven.Client.ServerWide;
using Sparrow;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    public class BucketMigrationReplication : ReplicationNode
    {
        public int Bucket;
        public int Shard;
        public readonly string Node;
        public readonly long MigrationIndex;

        public BucketMigrationReplication(string node, long migrationIndex)
        {
            Node = node ?? throw new ArgumentNullException(nameof(node));
            MigrationIndex = migrationIndex;
        }

        public bool ForBucketMigration(BucketMigration migration)
        {
            if (migration.MigrationIndex != MigrationIndex)
                return false;

            if (migration.Bucket != Bucket)
                return false;

            if (migration.DestinationShard != Shard)
                return false;

            return true;
        }

        public override int GetHashCode() => (int)(CalculateStringHash(Node) ^ (ulong)Hashing.Mix(MigrationIndex));

        public override string FromString() => $"Migrating bucket '{Bucket}' to shard '{Shard}' @ {MigrationIndex}";

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Bucket)] = Bucket;
            json[nameof(Shard)] = Shard;
            json[nameof(MigrationIndex)] = MigrationIndex;
            json[nameof(Node)] = Node;
            return json;
        }
    }
}
