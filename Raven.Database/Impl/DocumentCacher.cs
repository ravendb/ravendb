using System;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Caching;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Database.Impl
{
    public class DocumentCacher : IDocumentCacher, ILowMemoryHandler
    {
        private readonly InMemoryRavenConfiguration configuration;
        private MemoryCache cachedSerializedDocuments;
        private static readonly ILog log = LogManager.GetCurrentClassLogger();
        
        [ThreadStatic]
        private static bool skipSetAndGetDocumentInCache;

        [ThreadStatic]
        private static bool skipSetDocumentInCache;

        public DocumentCacher(InMemoryRavenConfiguration configuration)
        {
            this.configuration = configuration;
            cachedSerializedDocuments = CreateCache();

            MemoryStatistics.RegisterLowMemoryHandler(this);
        }

        private MemoryCache CreateCache()
        {
            var result = new MemoryCache(typeof(DocumentCacher).FullName + ".Cache", new NameValueCollection
            {
                {"physicalMemoryLimitPercentage", configuration.MemoryCacheLimitPercentage.ToString()},
                {"pollingInterval",  configuration.MemoryCacheLimitCheckInterval.ToString(@"hh\:mm\:ss")},
                {"cacheMemoryLimitMegabytes", configuration.MemoryCacheLimitMegabytes.ToString()}
            });
            log.Info(@"MemoryCache Settings:
  PhysicalMemoryLimit = {0}
  CacheMemoryLimit    = {1}
  PollingInterval     = {2}", result.PhysicalMemoryLimit, result.CacheMemoryLimit, result.PollingInterval);

            return result;
        }

        public void HandleLowMemory()
        {
            var oldCache = cachedSerializedDocuments;

            cachedSerializedDocuments = CreateCache();

            oldCache.Dispose();
        }

        public void SoftMemoryRelease()
        {
            
        }

        public LowMemoryHandlerStatistics GetStats()
        {
            return new LowMemoryHandlerStatistics
            {
                Name = "DocumentCacher",
                EstimatedUsedMemory = cachedSerializedDocuments.Sum(x =>
                {
                    CachedDocument curDocument;
                    if (x.Value is CachedDocument)
                    {
                        curDocument = x.Value as CachedDocument;
                        return curDocument.Size;
                    }
                    return 0;
                }),
                DatabaseName = configuration.DatabaseName,
                Metadata = new
                {
                    CachedDocuments = cachedSerializedDocuments.GetCount()
                }
            };
        }

        public static IDisposable SkipSetAndGetDocumentsInDocumentCache()
        {
            var old = skipSetAndGetDocumentInCache;
            skipSetAndGetDocumentInCache = true;

            return new DisposableAction(() => skipSetAndGetDocumentInCache = old);
        }

        public static IDisposable SkipSetDocumentsInDocumentCache()
        {
            var old = skipSetDocumentInCache;
            skipSetDocumentInCache = true;

            return new DisposableAction(() => skipSetDocumentInCache = old);
        }

        public CachedDocument GetCachedDocument(string key, Etag etag)
        {
            if (skipSetAndGetDocumentInCache)
                return null;

            CachedDocument cachedDocument;
            try
            {
                cachedDocument = (CachedDocument)cachedSerializedDocuments.Get("Doc/" + key + "/" + etag);
            }
            catch (OverflowException)
            {
                // this is a bug in the framework
                // http://connect.microsoft.com/VisualStudio/feedback/details/735033/memorycache-set-fails-with-overflowexception-exception-when-key-is-u7337-u7f01-u2117-exception-message-negating-the-minimum-value-of-a-twos-complement-number-is-invalid 
                // in this case, we just treat it as uncacheable
                return null;
            }
            if (cachedDocument == null)
                return null;
            return new CachedDocument
            {
                Document = (RavenJObject)cachedDocument.Document.CreateSnapshot(),
                Metadata = (RavenJObject)cachedDocument.Metadata.CreateSnapshot(),
                Size = cachedDocument.Size
            };
        }

        public void SetCachedDocument(string key, Etag etag, ref RavenJObject doc, ref RavenJObject metadata, int size)
        {
            if (skipSetAndGetDocumentInCache)
                return;

            if (skipSetDocumentInCache)
                return;

            var cacheKey = "Doc/" + key + "/" + etag;
            try
            {
                if (cachedSerializedDocuments.Get(cacheKey) != null)
                    return;
            }
            catch(OverflowException)
            {
                // this is a bug in the framework
                // http://connect.microsoft.com/VisualStudio/feedback/details/735033/memorycache-set-fails-with-overflowexception-exception-when-key-is-u7337-u7f01-u2117-exception-message-negating-the-minimum-value-of-a-twos-complement-number-is-invalid 
                // in this case, we just treat it as uncacheable
            }

            var documentClone = doc;
            documentClone.EnsureCannotBeChangeAndEnableSnapshotting();
            var metadataClone = metadata;
            metadataClone.EnsureCannotBeChangeAndEnableSnapshotting();

            doc = (RavenJObject)documentClone.CreateSnapshot();
            metadata = (RavenJObject) metadata.CreateSnapshot();

            try
            {
                cachedSerializedDocuments.Set(cacheKey, new CachedDocument
                {
                    Document = documentClone,
                    Metadata = metadataClone,
                    Size = size
                }, new CacheItemPolicy
                {
                    SlidingExpiration = configuration.MemoryCacheExpiration,
                });
            }
            catch (OverflowException)
            {
                // this is a bug in the framework
                // http://connect.microsoft.com/VisualStudio/feedback/details/735033/memorycache-set-fails-with-overflowexception-exception-when-key-is-u7337-u7f01-u2117-exception-message-negating-the-minimum-value-of-a-twos-complement-number-is-invalid 
                // in this case, we just treat it as uncacheable
            }

        }

        public void RemoveCachedDocument(string key, Etag etag)
        {
            try
            {
                cachedSerializedDocuments.Remove("Doc/" + key + "/" + etag);
            }
            catch (OverflowException)
            {
                // this is a bug in the framework
                // http://connect.microsoft.com/VisualStudio/feedback/details/735033/memorycache-set-fails-with-overflowexception-exception-when-key-is-u7337-u7f01-u2117-exception-message-negating-the-minimum-value-of-a-twos-complement-number-is-invalid 
                // in this case, we just treat it as uncacheable
            }
        }

        public object GetStatistics()
        {
            long v = 0;
            foreach (var kvp in cachedSerializedDocuments)
            {
                var cachedDocument = kvp.Value as CachedDocument;
                if (cachedDocument != null)
                    v += cachedDocument.Size;
            }
            return new
            {
                cachedSerializedDocuments.CacheMemoryLimit,
                cachedSerializedDocuments.DefaultCacheCapabilities,
                cachedSerializedDocuments.PhysicalMemoryLimit,
                cachedSerializedDocuments.PollingInterval,
                CachedItems = cachedSerializedDocuments.GetCount(),
                Size = v,
                HumaneSize = SizeHelper.Humane(v)
            };
        }

        public void Dispose()
        {
            cachedSerializedDocuments.Dispose();
        }
    }
}
