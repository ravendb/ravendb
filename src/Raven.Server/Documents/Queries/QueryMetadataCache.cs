using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Queries.Parser;
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

            if (TryGetQueryMetadataHash(query, context, out metadataHash) == false)
                return false;

            metadata = _cache[metadataHash % CacheSize];
            if (metadata == null || metadata.CacheKey != metadataHash)
                return false;

            return true;
        }

        public void MaybeAddToCache(QueryMetadata metadata, string indexName)
        {
            if (metadata.CanCache == false)
                return;

            if (metadata.IsDynamic)
                metadata.DynamicIndexName = indexName;

            _cache[metadata.CacheKey % CacheSize] = metadata;
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
                    if (tokenType == ValueTokenType.Null)
                        return false;

                    hasher.Write((int)tokenType);
                }

                hash = hasher.GetHash();
                return true;
            }
        }
    }
}
