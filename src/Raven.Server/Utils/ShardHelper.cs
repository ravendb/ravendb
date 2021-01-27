using System;

namespace Raven.Server.Utils
{
    public static class ShardHelper
    {
        public static int TryGetShardIndexAndDatabaseName(ref string name)
        {
            int shardIndex = name.IndexOf('$');
            if (shardIndex != -1)
            {
                var slice = name.AsSpan().Slice(shardIndex + 1);
                name = name.Substring(0, shardIndex);
                if (int.TryParse(slice, out shardIndex) == false)
                    throw new ArgumentNullException(nameof(name), "Unable to parse sharded database name: " + name);
            }

            return shardIndex;
        }

        public static int TryGetShardIndex(string name)
        {
            int shardIndex = name.IndexOf('$');
            if (shardIndex != -1)
            {
                var slice = name.AsSpan().Slice(shardIndex + 1);
                if (int.TryParse(slice, out shardIndex) == false)
                    throw new ArgumentNullException(nameof(name), "Unable to parse sharded database name: " + name);
            }

            return shardIndex;
        }

        public static string ToDatabaseName(string shardName)
        {
            int shardIndex = shardName.IndexOf('$');
            if (shardIndex == -1)
                return shardName;

            return shardName.Substring(0, shardIndex);
        }

        public static bool IsShardedName(string name)
        {
            return name.IndexOf('$') != -1;
        }
    }
}
