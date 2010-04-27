using System;
using System.Threading;
using Newtonsoft.Json.Linq;
using System.Linq;
using Raven.Database.Json;

namespace Raven.Database.Cache
{
	public class JsonCache : CacheBuildingBlock<JObject>
	{
		public static void RememberDocument(Guid etag, JObject value)
		{
			Remember("docs/" + etag, value);
		}

		public static void RememberMetadata(Guid etag, JObject value)
		{
			Remember("metadata/" + etag, value);
		}

		public static JObject ParseDocument(Guid etag, byte[] data)
		{
			return new JObject(Parse("docs/" + etag, () => data.ToJObject())); // force deep cloning
		}

		public static JObject ParseMetadata(Guid etag, byte[] data)
		{
			return new JObject(Parse("metadata/" + etag, () => data.ToJObject())); // force deep cloning
		}
	}
}