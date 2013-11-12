using Raven.Abstractions.Extensions;

namespace Raven.Database.Storage.Voron.StorageActions
{
	using System.Linq;

	public static class Util
	{
		public const string MetadataSuffix = "metadata";
		public const string DataSuffix = "data";

		public static string OriginalKey(string dataOrMetadataKey)
		{
		    var keyParts = dataOrMetadataKey.Split('/').ToHashSet();
		    keyParts.RemoveWhere(str => str == keyParts.Last());
		    return string.Join("/",keyParts);
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
