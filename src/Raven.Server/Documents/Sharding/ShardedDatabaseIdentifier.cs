using System;
using JetBrains.Annotations;

namespace Raven.Server.Documents.Sharding;

public readonly struct ShardedDatabaseIdentifier
{
    public readonly string NodeTag;

    public readonly int ShardNumber;

    public ShardedDatabaseIdentifier([NotNull] string nodeTag, int shardNumber)
    {
        NodeTag = nodeTag ?? throw new ArgumentNullException(nameof(nodeTag));
        ShardNumber = shardNumber;
    }

    public bool Equals(ShardedDatabaseIdentifier other)
    {
        return string.Equals(NodeTag, other.NodeTag, StringComparison.OrdinalIgnoreCase) && ShardNumber == other.ShardNumber;
    }

    public override bool Equals(object obj)
    {
        return obj is ShardedDatabaseIdentifier other && Equals(other);
    }

    public override int GetHashCode()
    {
        var hashCode = new HashCode();
        hashCode.Add(NodeTag, StringComparer.OrdinalIgnoreCase);
        hashCode.Add(ShardNumber);
        return hashCode.ToHashCode();
    }
}
