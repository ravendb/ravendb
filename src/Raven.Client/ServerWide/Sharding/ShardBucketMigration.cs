using System;
using System.Collections.Generic;
using Sparrow;

namespace Raven.Client.ServerWide.Sharding;

public class ShardBucketMigration : IDatabaseTask
{
    public MigrationStatus Status;
    public int Bucket;
    public int SourceShard;
    public int DestinationShard;
    public long MigrationIndex;
    public long? ConfirmationIndex;
    public string LastSourceChangeVector;

    public List<string> ConfirmedDestinations = new List<string>();
    public List<string> ConfirmedSourceCleanup = new List<string>();

    public string MentorNode;
    public override string ToString()
    {
        return $"Bucket '{Bucket}' is migrated from '{SourceShard}' to '{DestinationShard}' at status '{Status}'.{Environment.NewLine}" +
               $"Migrations index '{MigrationIndex}', Last change vector from source '{LastSourceChangeVector}', " +
               $"Propagated to {string.Join(", ", ConfirmedDestinations)} and confirmed at {ConfirmationIndex}, Cleaned up at {string.Join(", ", ConfirmedSourceCleanup)}";
    }

    private ulong? _hashCode;

    public ulong GetTaskKey()
    {
        if (_hashCode.HasValue)
            return _hashCode.Value;

        var hash = Hashing.Combine((ulong)SourceShard, (ulong)DestinationShard);
        hash = Hashing.Combine(hash, (ulong)Bucket);
        _hashCode = Hashing.Combine(hash, (ulong)MigrationIndex);

        return _hashCode.Value;
    }

    public string GetMentorNode() => MentorNode;

    public string GetDefaultTaskName() => GetTaskName();

    public string GetTaskName() => $"Bucket '{Bucket}' migration from '{SourceShard}' to '{DestinationShard}' @ {MigrationIndex}";

    public bool IsResourceIntensive() => false;
}
