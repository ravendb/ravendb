using System;
using System.Collections.Generic;
using System.Text;
using Sparrow;

namespace Raven.Client.ServerWide.Sharding;

public sealed class ShardBucketMigration : IDatabaseTask
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
        var sb = new StringBuilder($"Bucket '{Bucket}' is migrating from '{SourceShard}' to '{DestinationShard}'. current status: '{Status}'.");
        switch (Status)
        {
            case MigrationStatus.Pending:
            case MigrationStatus.Moving:
                break;
            case MigrationStatus.Moved:
            case MigrationStatus.OwnershipTransferred:
                sb.AppendLine($"Migrations index '{MigrationIndex}', Last change vector from source '{LastSourceChangeVector}',");
                sb.AppendLine(
                    $"Propagated to {string.Join(", ", ConfirmedDestinations)} and confirmed at {ConfirmationIndex}, Cleaned up at {string.Join(", ", ConfirmedSourceCleanup)}");
                return sb.ToString();
            default:
                throw new ArgumentOutOfRangeException(nameof(Status));
        }
        return sb.ToString();
    }

    public bool IsActive => Status == MigrationStatus.Moved || Status == MigrationStatus.Moving;

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

    public bool IsPinnedToMentorNode() => false;
}
