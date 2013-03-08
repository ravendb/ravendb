using System;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Impl
{
	public interface IDocumentCacher : IDisposable
	{
		CachedDocument GetCachedDocument(string key, Etag etag);
		void SetCachedDocument(string key, Etag etag, RavenJObject doc, RavenJObject metadata, int size);
		void RemoveCachedDocument(string key, Etag etag);
	}
}