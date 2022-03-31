using System;
using System.Text;

namespace Tests.Infrastructure.Utils;

public class UniqueDatabaseInstanceKey
{
    public string NodeTag { get; }

    public int? ShardNumber { get; }

    public UniqueDatabaseInstanceKey(string nodeTag)
    {
        NodeTag = nodeTag ?? throw new ArgumentNullException(nameof(nodeTag));
    }

    private UniqueDatabaseInstanceKey(string nodeTag, int shardNumber)
    {
        NodeTag = nodeTag;
        ShardNumber = shardNumber;
    }

    public UniqueDatabaseInstanceKey ForShard(int shardNumber)
    {
        return new UniqueDatabaseInstanceKey(NodeTag, shardNumber);
    }

    protected bool Equals(UniqueDatabaseInstanceKey other)
    {
        return NodeTag == other.NodeTag && ShardNumber == other.ShardNumber;
    }

    public override bool Equals(object obj)
    {
        if (ReferenceEquals(null, obj))
            return false;
        if (ReferenceEquals(this, obj))
            return true;
        if (obj.GetType() != this.GetType())
            return false;
        return Equals((UniqueDatabaseInstanceKey)obj);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(NodeTag, ShardNumber);
    }

    public override string ToString()
    {
        var builder = new StringBuilder();
        builder.Append($"{nameof(NodeTag)} = {NodeTag}");
        if (ShardNumber != null)
            builder.Append($", {nameof(ShardNumber)} = {ShardNumber}");

        return builder.ToString();
    }
}
