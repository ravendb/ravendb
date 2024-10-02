using Raven.Client.Util;
using Sparrow;

namespace Raven.Server.ServerWide.Maintenance.Sharding
{
    public sealed class ShardMigrationResult
    {
        public string Database;
        public string RaftId = RaftIdGenerator.NewId();

        public int SourceShard;
        public int DestinationShard;
        public int Bucket;

        public override string ToString()
        {
            return $"Migrate bucket '{Bucket}' from '{SourceShard}' to '{DestinationShard}'";
        }

        public override int GetHashCode() => (int)Hashing.XXHash64.Calculate(new int[] { SourceShard, DestinationShard, Bucket });

        public override bool Equals(object obj)
        {
            if (obj == null)
                return false;
            
            if (obj is ShardMigrationResult result == false)
                return false;


            return result.SourceShard == SourceShard && 
                   result.DestinationShard == DestinationShard && 
                   result.Bucket == Bucket;

        }
    }
}
