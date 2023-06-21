using System.Text;
using Corax;
using Corax.Mappings;
using Corax.Utils;
using Corax.Utils.Spatial;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Sharding.Queries;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.Utils;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Sharding.Persistence.Corax;

public sealed class ShardedCoraxIndexReadOperation : CoraxIndexReadOperation
{
    public ShardedCoraxIndexReadOperation(Index index, Logger logger, Transaction readTransaction, QueryBuilderFactories queryBuilderFactories,
        IndexFieldsMapping fieldsMapping, IndexQueryServerSide query) : base(index, logger, readTransaction, queryBuilderFactories, fieldsMapping, query)
    {
    }

    protected override QueryResult CreateQueryResult<TDistinct, THasProjection, THighlighting>(ref IdentityTracker<TDistinct> tracker, Document document,
        IndexQueryServerSide query,
        DocumentsOperationContext documentsContext, long indexEntryId, OrderMetadata[] orderByFields, ref THighlighting highlightings, Reference<long> skippedResults,
        ref THasProjection hasProjections,
        ref bool markedAsSkipped)
    {
        var result = base.CreateQueryResult(ref tracker, document, query, documentsContext, indexEntryId, orderByFields, ref highlightings, skippedResults, ref hasProjections, ref markedAsSkipped);

        if (result.Result != null && query.ReturnOptions != null)
        {
            if (query.ReturnOptions.AddOrderByFieldsMetadata)
            {
                if (_index.Type.IsMapReduce() == false) // for a map-reduce index the returned results already have fields that are used for sorting
                    result.Result = AddOrderByFields(result.Result, query, indexEntryId, orderByFields);
            }

            if (query.ReturnOptions.AddDataHashMetadata) 
                result.Result = result.Result.EnsureDataHashInQueryResultMetadata();
        }

        return result;
    }

    private ShardedQueryResultDocument AddOrderByFields(Document queryResult, IndexQueryServerSide query, long indexEntryId, OrderMetadata[] orderByFields)
    {
        var result = ShardedQueryResultDocument.From(queryResult);

        for (int i = 0; i < query.Metadata.OrderBy.Length; i++)
        {
            var orderByField = query.Metadata.OrderBy[i];

            if (orderByField.OrderingType == OrderByFieldType.Random)
                break; // we order by random when merging results from shards

            if (orderByField.OrderingType == OrderByFieldType.Score)
            {
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal, "RavenDB-13927 Order by score");
                throw new NotSupportedInShardingException("Ordering by score is not supported in sharding");
            }

            var orderByFieldMetadata = orderByFields[i];

            IndexEntryReader entryReader = _indexSearcher.GetEntryReaderFor(indexEntryId);
            IndexEntryReader.FieldReader reader = entryReader.GetFieldReaderFor(orderByFieldMetadata.Field);

            switch (orderByField.OrderingType)
            {
                case OrderByFieldType.Long:
                    reader.Read<long>(out var longValue);
                    result.AddLongOrderByField(longValue);
                    break;
                case OrderByFieldType.Double:
                    reader.Read<double>(out var doubleValue);
                    result.AddDoubleOrderByField(doubleValue);
                    break;
                case OrderByFieldType.Distance:
                {
                        var spatialReader = _indexSearcher.SpatialReader(orderByFieldMetadata.Field.FieldName);
                        double distance;
                        if (spatialReader.TryGetSpatialPoint(indexEntryId, out var coords) == false)
                        {
                            distance = orderByFieldMetadata.Ascending == false ? double.MinValue : double.MaxValue;
                        }
                        else
                        {
                            distance = SpatialUtils.GetGeoDistance(coords, (orderByFieldMetadata.Point.X, orderByFieldMetadata.Point.Y), orderByFieldMetadata.Round, orderByFieldMetadata.Units);
                        }
                        
                        result.AddDoubleOrderByField(distance);
                        break;
                    }
                default:
                    {
                        reader.Read(out var sv);
                        var stringValue = Encoding.UTF8.GetString(sv);
                        result.AddStringOrderByField(stringValue);
                        break;
                    }
            }
        }

        return result;
    }
    
    internal override void AssertCanOrderByScoreAutomaticallyWhenBoostingIsInvolved() => throw new NotSupportedInShardingException($"Ordering by score is not supported in sharding. You received this exception because your index has boosting, and we attempted to sort the results since the configuration `{RavenConfiguration.GetKey(i => i.Indexing.OrderByScoreAutomaticallyWhenBoostingIsInvolved)}` is enabled.");
}
