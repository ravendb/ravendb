namespace Raven.Client.Util
{
    internal static class ClientShardHelper
    {
        public static string ToShardName(string database, int shard)
        {
            var name = ToDatabaseName(database);

            int shardIndex = name.IndexOf('$');
            if (shardIndex == -1)
                return $"{name}${shard}";

            return name;
        }

        public static string ToDatabaseName(string shardName)
        {
            int shardIndex = shardName.IndexOf('$');
            if (shardIndex == -1)
                return shardName;

            return shardName.Substring(0, shardIndex);
        }
    }
}
