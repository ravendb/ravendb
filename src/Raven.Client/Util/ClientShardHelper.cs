namespace Raven.Client.Util
{
    internal static class ClientShardHelper
    {
        public static string ToShardName(string database, int shardNumber)
        {
            var name = ToDatabaseName(database);

            int shardNumberPosition = name.IndexOf('$');
            if (shardNumberPosition == -1)
                return $"{name}${shardNumber}";

            return name;
        }

        public static string ToDatabaseName(string shardName)
        {
            int shardNumberPosition = shardName.IndexOf('$');
            if (shardNumberPosition == -1)
                return shardName;

            return shardName.Substring(0, shardNumberPosition);
        }
    }
}
