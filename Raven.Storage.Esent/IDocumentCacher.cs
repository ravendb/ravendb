using System;
using Newtonsoft.Json.Linq;

namespace Raven.Storage.Esent
{
	public interface IDocumentCacher
	{
		Tuple<JObject, JObject> GetCachedDocument(string key, Guid etag);
		void SetCachedDocument(string key, Guid etag, Tuple<JObject, JObject> doc);
	}
}