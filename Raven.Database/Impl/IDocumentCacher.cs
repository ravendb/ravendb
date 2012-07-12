using System;
using Raven.Json.Linq;

namespace Raven.Database.Impl
{
	public interface IDocumentCacher : IDisposable
	{
		CachedDocument GetCachedDocument(string key, Guid etag);
		void SetCachedDocument(string key, Guid etag, RavenJObject doc, RavenJObject metadata, int size);
		void RemoveCachedDocument(string key, Guid etag);
	}
}