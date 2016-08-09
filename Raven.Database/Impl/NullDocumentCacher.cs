using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Impl
{
    public class NullDocumentCacher : IDocumentCacher
    {
        public void Dispose()
        {
        }

        public CachedDocument GetCachedDocument(string key, Etag etag)
        {
            return null;
        }

        public void SetCachedDocument(string key, Etag etag, ref RavenJObject doc, ref RavenJObject metadata, int size)
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
