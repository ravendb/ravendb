using System;
using Raven.Client.Documents.Queries;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class QueryMetadataCache
    {
        private const int CacheSize = 512;

        private readonly QueryMetadata[] _cache = new QueryMetadata[CacheSize];

        public bool TryGetMetadata(IndexQueryBase<BlittableJsonReaderObject> query, out ulong metadataHash, out QueryMetadata metadata)
        {
            metadataHash = 0;
            metadata = null;

            if (query == null || query.Query == null)
                return false;

            metadataHash = GetQueryMetadataHash(query);

            metadata = _cache[metadataHash % CacheSize];
            if (metadata == null)
                return false;

            if (metadata.CacheKey != metadataHash)
            {
                var nextProbe = Hashing.Mix(metadataHash) % CacheSize;
                metadata = _cache[nextProbe];
                if (metadata == null || metadata.CacheKey != metadataHash)
                    return false;
            }

            // we don't compare the query parameters because they don't matter
            // for the query plan that we use, at any rate, they will either error
            // if the query uses them and it is missing or they are there and will
            // noop because they aren't being used
            var shouldUseCachedItem = (query.Query == metadata.QueryText);

            if (shouldUseCachedItem)
            {
                metadata.LastQueriedAt = DateTime.UtcNow;
            }
            return shouldUseCachedItem;
        }

        public void MaybeAddToCache(QueryMetadata metadata, string indexName)
        {
            if (metadata.CanCache == false)
                return;

            if (metadata.IsDynamic)
                metadata.AutoIndexName = indexName;

            // we are intentionally racy here, to avoid locking
            var bestLocation = metadata.CacheKey % CacheSize;
            var existing = _cache[bestLocation];
            if (existing == null)
            {
                _cache[bestLocation] = metadata;
                return;
            }
            if (existing.CacheKey == metadata.CacheKey)
                return; // another copy

            var nextProbe = Hashing.Mix(metadata.CacheKey) % CacheSize;
            existing = _cache[nextProbe];
            if (existing == null) // use the probe location
            {
                _cache[nextProbe] = metadata;
                return;
            }
            // both are full, need to kick one out, we use GetHashCode as effective random value
            var loc = metadata.GetHashCode() % 2 == 0 ? bestLocation : nextProbe;
            _cache[loc] = metadata;
        }

        private static ulong GetQueryMetadataHash(IndexQueryBase<BlittableJsonReaderObject> query)
        {
            var hash = Hashing.XXHash64.CalculateRaw(query.Query);
            if (query.QueryParameters == null || query.QueryParameters.Count == 0)
                return hash;

            return Hashing.Combine(hash, query.QueryParameters.GetHashOfPropertyNames());
        }

        public QueryMetadata[] GetQueryCache()
        {
            return _cache;
        }
    }
}
