using System;
using System.Linq;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Sharding
{
    public class DestinationMigrationConfirmCommand : UpdateDatabaseCommand
    {
        public int Bucket;
        public long MigrationIndex;
        public string Node;

        public DestinationMigrationConfirmCommand(){}

        public DestinationMigrationConfirmCommand(int bucket, long migrationIndex, string node, string database, string raftId) : base(database, raftId)
        {
            Bucket = bucket;
            MigrationIndex = migrationIndex;
            Node = node;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (record.ShardBucketMigrations.TryGetValue(Bucket, out var migration) == false)
                throw new InvalidOperationException($"Bucket '{Bucket}' not found in the migration buckets");

            if (migration.MigrationIndex != MigrationIndex)
                throw new InvalidOperationException($"Wrong migration index. Expected: '{MigrationIndex}', Actual: '{migration.MigrationIndex}'");

            if (migration.Status != MigrationStatus.Moved)
                throw new InvalidOperationException($"Expected status is '{MigrationStatus.Moved}', Actual '{migration.Status}'");

            if (migration.ConfirmedDestinations.Contains(Node) == false)
                migration.ConfirmedDestinations.Add(Node);

            var shardTopology = record.Shards[migration.DestinationShard];
            if (shardTopology.Members.All(migration.ConfirmedDestinations.Contains))
            {
                migration.Status = MigrationStatus.OwnershipTransferred;
                migration.ConfirmationIndex = etag;
                record.MoveBucket(migration.Bucket, migration.DestinationShard);
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Node)] = Node;
            json[nameof(Bucket)] = Bucket;
            json[nameof(MigrationIndex)] = MigrationIndex;
        }
    }
}
