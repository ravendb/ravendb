using System;
using System.Runtime.Caching;
using Raven.Json.Linq;

namespace Raven.Database.Impl
{
	public interface IDocumentCacher : IDisposable
	{
		Tuple<RavenJObject, RavenJObject> GetCachedDocument(string key, Guid etag);
		void SetCachedDocument(string key, Guid etag, RavenJObject doc, RavenJObject metadata);
	}

    public class DocumentCacher : IDocumentCacher
    {
        private readonly MemoryCache cachedSerializedDocuments = new MemoryCache(typeof(DocumentCacher).FullName + ".Cache");

        public Tuple<RavenJObject, RavenJObject> GetCachedDocument(string key, Guid etag)
        {
            var cachedDocument = (Tuple<RavenJObject, RavenJObject>)cachedSerializedDocuments.Get("Doc/" + key + "/" + etag);
            if (cachedDocument == null)
                return null;
            return Tuple.Create(cachedDocument.Item1.CreateSnapshot(), cachedDocument.Item2.CreateSnapshot());
        }

        public void SetCachedDocument(string key, Guid etag, RavenJObject doc, RavenJObject metadata)
        {
            doc = new RavenJObject(doc);
            doc.EnsureSnapshot();
            metadata = new RavenJObject(metadata);
            metadata.EnsureSnapshot();
            cachedSerializedDocuments["Doc/" + key + "/" + etag] = Tuple.Create(doc,metadata);
        }

        public void Dispose()
        {
            cachedSerializedDocuments.Dispose();
        }
    }
}