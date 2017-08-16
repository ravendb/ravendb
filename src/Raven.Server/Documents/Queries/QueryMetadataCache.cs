using System.Collections.Generic;
using Raven.Client.Documents.Queries;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class QueryMetadataCache
    {
        private const int CacheSize = 512;

        private readonly Dictionary<ulong, QueryMetadata>[] _cache = new Dictionary<ulong, QueryMetadata>[CacheSize];

        public bool TryGetMetadata(IndexQueryBase<BlittableJsonReaderObject> query, JsonOperationContext context, out ulong metadataHash, out QueryMetadata metadata)
        {
            metadataHash = 0;
            metadata = null;

            if (query == null || query.Query == null || query.QueryParameters == null || query.QueryParameters.Count == 0)
                return false;

            if (TryGetQueryMetadataHash(query, context, out metadataHash) == false)
                return false;

            var dictionary = _cache[metadataHash % CacheSize];
            if (dictionary == null || dictionary.TryGetValue(metadataHash, out metadata) == false)
                return false;

            return true;
        }

        public void MaybeAddToCache(QueryMetadata metadata, string indexName)
        {
            if (metadata.CanCache == false)
                return;

            if (metadata.IsDynamic)
                metadata.DynamicIndexName = indexName;

            var cacheKey = metadata.CacheKey % CacheSize;
            var dictionary = _cache[cacheKey];
            if (dictionary != null && dictionary.ContainsKey(metadata.CacheKey))
                return;

            var newDictionary = dictionary == null
                ? new Dictionary<ulong, QueryMetadata>()
                : new Dictionary<ulong, QueryMetadata>(dictionary);
            newDictionary[metadata.CacheKey] = metadata;

            _cache[cacheKey] = newDictionary;
        }

        private static bool TryGetQueryMetadataHash(IndexQueryBase<BlittableJsonReaderObject> query, JsonOperationContext context, out ulong hash)
        {
            hash = 0;

            using (var hasher = new QueryHashCalculator(context))
            {
                hasher.Write(query.Query);

                var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                foreach (var index in query.QueryParameters.GetPropertiesByInsertionOrder())
                {
                    query.QueryParameters.GetPropertyByIndex(index, ref propertyDetails);

                    var tokenType = QueryBuilder.GetValueTokenType(propertyDetails.Value, query.Query, query.QueryParameters, unwrapArrays: true);
                    hasher.Write((int)tokenType);
                }

                hash = hasher.GetHash();
                return true;
            }
        }
    }
}
