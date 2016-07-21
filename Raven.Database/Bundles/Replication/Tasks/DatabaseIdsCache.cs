using System;
using System.Collections.Concurrent;
using System.Threading;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Replication.Tasks
{
    public class DatabaseIdsCache: IDisposable
    {
        private readonly DocumentDatabase documentDatabase;
        private readonly ILog log;
        private readonly DatabaseIdsCacheDocument databaseIdsCacheDocument;
        private readonly Timer saveDocumentTimer;
        private const string CacheDocumentId = "Raven/Replication/DatabaseIdsCache";
        private bool disposed;
        private long version;
        private long snapshotVersion;

        public DatabaseIdsCache(DocumentDatabase documentDatabase, ILog log)
        {
            this.documentDatabase = documentDatabase;
            this.log = log;

            databaseIdsCacheDocument = GetCachedDocument();
            if (databaseIdsCacheDocument == null)
            {
                databaseIdsCacheDocument = new DatabaseIdsCacheDocument();
                documentDatabase.Documents.Put(CacheDocumentId, null,
                    RavenJObject.FromObject(databaseIdsCacheDocument), new RavenJObject(), null);
            }

            saveDocumentTimer = new Timer(SaveDocument, null, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(15));
        }

        private DatabaseIdsCacheDocument GetCachedDocument()
        {
            var jsonDocument = documentDatabase.Documents.Get(CacheDocumentId, null);
            if (jsonDocument == null)
                return null;

            DatabaseIdsCacheDocument cachedDocument = null;

            try
            {
                cachedDocument = jsonDocument.DataAsJson.JsonDeserialization<DatabaseIdsCacheDocument>();
            }
            catch (Exception e)
            {
                log.WarnException("Failed to get replication destinations database ids, creating a new one", e);
            }

            return cachedDocument;
        }

        public void Add(string url, Guid? databaseId)
        {
            if (databaseId == null)
                return;

            var updated = true;
            databaseIdsCacheDocument.UrlToDatabaseId.AddOrUpdate(url, databaseId.Value,
                (_, oldDatabaseId) =>
                {
                    if (oldDatabaseId.Equals(databaseId))
                        updated = false;

                    return databaseId.Value;
                });

            if (updated == false)
                return;

            Interlocked.Increment(ref version);
        }

        public Guid? Get(string url)
        {
            Guid databaseId;

            if (databaseIdsCacheDocument.UrlToDatabaseId.TryGetValue(url, out databaseId) == false)
            {
                return null;
            }

            return databaseId;
        }

        private void SaveDocument(object _)
        {
            if (disposed)
                return;

            var currentVersion = Interlocked.Read(ref version);
            if (currentVersion == snapshotVersion)
                return;

            try
            {
                documentDatabase.Documents.Put(CacheDocumentId, null,
                    RavenJObject.FromObject(databaseIdsCacheDocument), new RavenJObject(), null);

                snapshotVersion = currentVersion;
            }
            catch (Exception e)
            {
                if (disposed)
                    return;

                log.WarnException("Failed to save replication destinations database ids", e);
            }
        }

        public class DatabaseIdsCacheDocument
        {
            public DatabaseIdsCacheDocument()
            {
                UrlToDatabaseId = new ConcurrentDictionary<string, Guid>();
            }

            public ConcurrentDictionary<string, Guid> UrlToDatabaseId { get; set; }
        }

        public void Dispose()
        {
            disposed = true;
            saveDocumentTimer.Dispose();
        }
    }
}