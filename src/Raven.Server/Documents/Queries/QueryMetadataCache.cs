using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Client.Documents.Queries;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class QueryMetadataCache
    {
        private const int CacheSize = 512;

        private readonly QueryMetadata[] _cache = new QueryMetadata[CacheSize];

        public bool TryGetMetadata(IndexQueryBase<BlittableJsonReaderObject> query, JsonOperationContext context, out ulong metadataHash, out QueryMetadata metadata)
        {
            metadataHash = 0;
            metadata = null;

            if (query == null || query.Query == null || query.QueryParameters == null || query.QueryParameters.Count == 0)
                return false;

            metadataHash = GetQueryMetadataHash(query);

            metadata = _cache[metadataHash % CacheSize];
            if (metadata == null)
                return false;
            //TODO: Here we assume that if the hashes are the same, then the values are the same
            //TODO: this isn't always the case, but the problem is that this is perf sensitive
            //TODO: place and we don't want to do a lot of comparisons all the time here
            if (metadata.CacheKey != metadataHash)
            {
                var nextProbe = Hashing.Mix(metadataHash) % CacheSize;
                metadata = _cache[nextProbe];
                if (metadata == null || metadata.CacheKey != metadataHash)
                    return false;
            }
            return true;
        }

        public void MaybeAddToCache(QueryMetadata metadata, string indexName)
        {
            if (metadata.CanCache == false)
                return;

            if (metadata.IsDynamic)
                metadata.DynamicIndexName = indexName;

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
            if (query.QueryParameters == null)
                return hash;
            return Hashing.Combine(hash, query.QueryParameters.GetHashOfPropertyNames());
        }
    }
}
