using System;
using Raven.Json.Linq;

namespace Raven.Storage.Esent
{
	public interface IDocumentCacher
	{
		Tuple<RavenJObject, RavenJObject> GetCachedDocument(string key, Guid etag);
		void SetCachedDocument(string key, Guid etag, Tuple<RavenJObject, RavenJObject> doc);
	}
}