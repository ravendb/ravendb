using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Impl
{
    public class NullDocumentCacher : IDocumentCacher
    {
        public void Dispose()
        {
        }

        public CachedDocument GetCachedDocument(string key, Etag etag, bool metadataOnly = false)
        {
            return null;
        }

        public void SetCachedDocument(string key, Etag etag, RavenJObject doc, RavenJObject metadata, int size)
        {
        }

        public void RemoveCachedDocument(string key, Etag etag)
        {
        }

        public object GetStatistics()
        {
            return new { };
        }
    }
}
