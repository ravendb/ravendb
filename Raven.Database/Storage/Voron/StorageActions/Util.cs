namespace Raven.Database.Storage.Voron.StorageActions
{
	using System.Linq;

	public static class Util
	{
		public const string MetadataSuffix = "metadata";
		public const string DataSuffix = "data";

		public static string OriginalKey(string dataOrMetadataKey)
		{
			return dataOrMetadataKey.Split('/').First();
		}

		public static string DataKey(string key)
		{
			return key + "/" + DataSuffix;
		}

		public static string MetadataKey(string key)
		{
			return key + "/" + MetadataSuffix;
		}

	}
}
