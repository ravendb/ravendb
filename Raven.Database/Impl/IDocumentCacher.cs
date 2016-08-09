using System;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Impl
{
    public interface IDocumentCacher : IDisposable
    {
        CachedDocument GetCachedDocument(string key, Etag etag);
        void SetCachedDocument(string key, Etag etag, ref RavenJObject doc, ref RavenJObject metadata, int size);
        void RemoveCachedDocument(string key, Etag etag);
        object GetStatistics();
    }
}
