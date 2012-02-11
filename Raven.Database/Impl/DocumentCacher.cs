using System;
using System.Collections.Specialized;
using System.Runtime.Caching;
using NLog;
using Raven.Abstractions.Extensions;
using Raven.Database.Config;
using Raven.Json.Linq;

namespace Raven.Database.Impl
{
	public class DocumentCacher : IDocumentCacher
	{
		private readonly InMemoryRavenConfiguration configuration;
		private readonly MemoryCache cachedSerializedDocuments;
		private static readonly Logger log = LogManager.GetCurrentClassLogger();
		
		[ThreadStatic]
		private static bool skipSettingDocumentInCache;

		public DocumentCacher(InMemoryRavenConfiguration configuration)
		{
			this.configuration = configuration;
			cachedSerializedDocuments = new MemoryCache(typeof(DocumentCacher).FullName + ".Cache", new NameValueCollection
			{
				{"physicalMemoryLimitPercentage", configuration.MemoryCacheLimitPercentage.ToString()},
				{"pollingInterval",  configuration.MemoryCacheLimitCheckInterval.ToString(@"hh\:mm\:ss")},
				{"cacheMemoryLimitMegabytes", configuration.MemoryCacheLimitMegabytes.ToString()}
			});
			log.Info(@"MemoryCache Settings:
  PhysicalMemoryLimit = {0}
  CacheMemoryLimit    = {1}
  PollingInterval     = {2}", cachedSerializedDocuments.PhysicalMemoryLimit, cachedSerializedDocuments.CacheMemoryLimit,
			  cachedSerializedDocuments.PollingInterval);
		}

		public static IDisposable SkipSettingDocumentsInDocumentCache()
		{
			var old = skipSettingDocumentInCache;
			skipSettingDocumentInCache = true;

			return new DisposableAction(() => skipSettingDocumentInCache = old);
		}

		public CachedDocument GetCachedDocument(string key, Guid etag)
		{
			var cachedDocument = (CachedDocument)cachedSerializedDocuments.Get("Doc/" + key + "/" + etag);
			if (cachedDocument == null)
				return null;
			return new CachedDocument
			{
				Document = cachedDocument.Document.CreateSnapshot(),
				Metadata = cachedDocument.Metadata.CreateSnapshot(),
				Size = cachedDocument.Size
			};
		}

		public void SetCachedDocument(string key, Guid etag, RavenJObject doc, RavenJObject metadata, int size)
		{
			if (skipSettingDocumentInCache)
				return;

			var documentClone = ((RavenJObject)doc.CloneToken());
			documentClone.EnsureSnapshot();
			var metadataClone = ((RavenJObject)metadata.CloneToken());
			metadataClone.EnsureSnapshot();
			cachedSerializedDocuments.Set("Doc/" + key + "/" + etag, new CachedDocument
			{
				Document = documentClone,
				Metadata = metadataClone,
				Size = size
			}, new CacheItemPolicy
			{
				SlidingExpiration = configuration.MemoryCacheExpiration,
			});

		}

		public void RemoveCachedDocument(string key, Guid etag)
		{
			cachedSerializedDocuments.Remove("Doc/" + key + "/" + etag);
		}

		public void Dispose()
		{
			cachedSerializedDocuments.Dispose();
		}
	}
}