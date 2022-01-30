using System;
using System.Linq;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class StartBucketMigrationCommand : UpdateDatabaseCommand
    {
        public int SourceShard;
        public int DestinationShard;
        public int Bucket;

        public StartBucketMigrationCommand()
        {
        }

        public StartBucketMigrationCommand(string database, string raftId) : base(database, raftId)
        {
            
        }
        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (record.BucketMigrations.Count > 0)
            {
                foreach (var migration in record.BucketMigrations)
                {
                    if (migration.Value.Status < MigrationStatus.OwnershipTransferred)
                        throw new InvalidOperationException(
                            $"Only one bucket can be transferred at a time, currently bucket {migration.Key} is {migration.Value.Status}");

                    if (migration.Key == Bucket)
                        throw new InvalidOperationException($"Can't migrate bucket {Bucket}, since it is still migrating.");
                }
            }

            record.BucketMigrations.Add(Bucket, new BucketMigration
            {
                Bucket = Bucket,
                DestinationShard = DestinationShard,
                SourceShard = SourceShard,
                MigrationIndex = etag,
                Status = MigrationStatus.Moving
            });
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(SourceShard)] = SourceShard;
            json[nameof(DestinationShard)] = DestinationShard;
            json[nameof(Bucket)] = Bucket;
        }
    }

    public class SourceMigrationSendCompletedCommand : UpdateDatabaseCommand
    {
        public int Bucket;
        public long MigrationIndex;

        public string LastSentChangeVector;

        public SourceMigrationSendCompletedCommand()
        {

        }

        public SourceMigrationSendCompletedCommand(string database, string raftId) : base(database, raftId)
        {
            
        }
        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (record.BucketMigrations.TryGetValue(Bucket, out var migration) == false)
                throw new InvalidOperationException($"Bucket '{Bucket}' not found in the migration buckets");

            if (migration.MigrationIndex != MigrationIndex)
                throw new InvalidOperationException($"Wrong migration index. Expected: '{MigrationIndex}', Actual: '{migration.MigrationIndex}'");

            if (migration.Status != MigrationStatus.Moving)
                throw new InvalidOperationException($"Expected status is '{MigrationStatus.Moving}', Actual '{migration.Status}'");

            migration.Status = MigrationStatus.Moved;
            migration.LastSourceChangeVector = LastSentChangeVector;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Bucket)] = Bucket;
            json[nameof(MigrationIndex)] = MigrationIndex;
            json[nameof(LastSentChangeVector)] = LastSentChangeVector;
        }
    }

    public class DestinationMigrationConfirmCommand : UpdateDatabaseCommand
    {
        public int Bucket;
        public long MigrationIndex;
        public string Node;

        public DestinationMigrationConfirmCommand(){}

        public DestinationMigrationConfirmCommand(string database, string raftId) : base(database, raftId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (record.BucketMigrations.TryGetValue(Bucket, out var migration) == false)
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

    public class SourceMigrationCleanupCommand : UpdateDatabaseCommand
    {
        public int Bucket;
        public long MigrationIndex;

        public string Node;

        public SourceMigrationCleanupCommand(){}

        public SourceMigrationCleanupCommand(string database, string raftId) : base(database, raftId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (record.BucketMigrations.TryGetValue(Bucket, out var migration) == false)
                throw new InvalidOperationException($"Bucket '{Bucket}' not found in the migration buckets");

            if (migration.MigrationIndex != MigrationIndex)
                throw new InvalidOperationException($"Wrong migration index. Expected: '{MigrationIndex}', Actual: '{migration.MigrationIndex}'");

            if (migration.Status != MigrationStatus.OwnershipTransferred)
                throw new InvalidOperationException($"Expected status is '{MigrationStatus.Moved}', Actual '{migration.Status}'");

            if (migration.ConfirmedSourceCleanup.Contains(Node) == false)
                migration.ConfirmedSourceCleanup.Add(Node);

            var shardTopology = record.Shards[migration.SourceShard];
            if (shardTopology.AllNodes.All(migration.ConfirmedSourceCleanup.Contains))
            {
                record.BucketMigrations.Remove(Bucket);
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
