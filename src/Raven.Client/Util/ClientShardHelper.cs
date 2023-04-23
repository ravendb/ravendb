using System;
using System.Diagnostics;
using System.Xml.Linq;

namespace Raven.Client.Util
{
    internal static class ClientShardHelper
    {
        public static string ToShardName(string database, int shardNumber)
        {
            if (IsShardName(database))
            {
                Debug.Assert(false, $"Expected a non shard name but got {database}");
                throw new ArgumentException($"Expected a non shard name but got {database}");
            }

            ResourceNameValidator.AssertValidDatabaseName(database);
            
            return $"{database}${shardNumber}";
        }

        public static string ToDatabaseName(string shardName)
        {
            int shardNumberPosition = shardName.IndexOf('$');
            if (shardNumberPosition == -1)
                return shardName;

            var databaseName = shardName.Substring(0, shardNumberPosition);
            ResourceNameValidator.AssertValidDatabaseName(databaseName);

            return databaseName;
        }

        public static bool TryGetShardNumberAndDatabaseName(string databaseName, out string shardedDatabaseName, out int shardNumber)
        {
            var index = databaseName.IndexOf('$');
            shardNumber = -1;

            if (index != -1)
            {
                var slice = databaseName.AsSpan().Slice(index + 1);
                shardedDatabaseName = databaseName.Substring(0, index);
                if (int.TryParse(slice.ToString(), out shardNumber) == false)
                    throw new ArgumentException(nameof(shardedDatabaseName), "Unable to parse sharded database name: " + shardedDatabaseName);

                return true;
            }

            shardedDatabaseName = databaseName;
            return false;
        }

        public static int? GetShardNumberFromDatabaseName(string databaseName)
        {
            if (TryGetShardNumberAndDatabaseName(databaseName, out _, out var shardNumber))
                return shardNumber;

            return null;
        }

        public static bool IsShardName(string shardName)
        {
            return shardName.IndexOf('$') != -1;
        }
    }
}
