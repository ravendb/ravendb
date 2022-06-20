using System;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Sharding
{
    public class SourceMigrationSendCompletedCommand : UpdateDatabaseCommand
    {
        public int Bucket;
        public long MigrationIndex;
        public string LastSentChangeVector;

        public SourceMigrationSendCompletedCommand()
        {

        }

        public SourceMigrationSendCompletedCommand(int bucket, long migrationIndex, string lastSentChangeVector, string database, string raftId) : base(database, raftId)
        {
            Bucket = bucket;
            MigrationIndex = migrationIndex;
            LastSentChangeVector = lastSentChangeVector;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (record.Sharding.BucketMigrations.TryGetValue(Bucket, out var migration) == false)
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
}
