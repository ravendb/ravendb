using System.Text;
using Corax;
using Corax.Mappings;
using Corax.Querying.Matches.SortingMatches.Meta;
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
using Sparrow.Server;
using Sparrow.Threading;
using Sparrow.Utils;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Sharding.Persistence.Corax;

public sealed class ShardedCoraxIndexReadOperation : CoraxIndexReadOperation
{
    public ShardedCoraxIndexReadOperation(Index index, RavenLogger logger, Transaction readTransaction, QueryBuilderFactories queryBuilderFactories,
        IndexFieldsMapping fieldsMapping, IndexQueryServerSide query) : base(index, logger, readTransaction, queryBuilderFactories, fieldsMapping, query)
    {
    }

    protected override QueryResult CreateQueryResult<TDistinct, THasProjection, THighlighting>(ref IdentityTracker<TDistinct> tracker, Document document,
        IndexQueryServerSide query,
        DocumentsOperationContext documentsContext, ref EntryTermsReader entryReader, FieldsToFetch highlightingFields, OrderMetadata[] orderByFields, 
        ref THighlighting highlightings, Reference<long> skippedResults,
        ref THasProjection hasProjections,
        ref bool markedAsSkipped)
    {
        var result = base.CreateQueryResult(ref tracker, document, query, documentsContext, ref entryReader, highlightingFields, orderByFields, ref highlightings, skippedResults, ref hasProjections, ref markedAsSkipped);
        if (result.Result == null || query.ReturnOptions == null) 
            return result;

        if (query.ReturnOptions.AddOrderByFieldsMetadata && _index.Type.IsMapReduce() == false)
        {
            // for a map-reduce index the returned results already have fields that are used for sorting
            result.Result = AddOrderByFields(result.Result, query, ref entryReader, orderByFields);
        }

        if (query.ReturnOptions.AddDataHashMetadata) 
            result.Result = result.Result.EnsureDataHashInQueryResultMetadata();

        return result;
    }

    private ShardedQueryResultDocument AddOrderByFields(Document queryResult, IndexQueryServerSide query, ref EntryTermsReader reader, OrderMetadata[] orderByFields)
    {
        var result = ShardedQueryResultDocument.From(queryResult);
        var currentCoraxOrderIndex = 0;

        // Number of order by fields in Corax index can be smaller than in query metadata
        // because we don't create OrderMetadata for fields with zero indexed terms
        for (int i = 0; i < query.Metadata.OrderBy.Length && currentCoraxOrderIndex < orderByFields.Length; i++)
        {
            var orderByField = query.Metadata.OrderBy[i];

            if (orderByField.OrderingType == OrderByFieldType.Random)
                break; // we order by random when merging results from shards

            if (orderByField.OrderingType == OrderByFieldType.Score)
            {
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal, "RavenDB-13927 Order by score");
                throw new NotSupportedInShardingException("Ordering by score is not supported in sharding");
            }

            var orderByFieldMetadata = orderByFields[currentCoraxOrderIndex];

            using (var byteStringContext = new ByteStringContext(SharedMultipleUseFlag.None))
            {
                Slice.From(byteStringContext, orderByField.Name.Value, ByteStringType.Immutable, out var orderByFieldNameSlice);
                
                if (SliceComparer.Compare(orderByFieldMetadata.Field.FieldName, orderByFieldNameSlice) != 0)
                    continue;
            }
                
            if (orderByFieldMetadata.Ascending != orderByField.Ascending)
                continue;

            if (IsSameOrderType(orderByFieldMetadata.FieldType, orderByField.OrderingType) == false)
                continue;

            currentCoraxOrderIndex++;
            
            reader.Reset();
            long fieldRootPage = IndexSearcher.FieldCache.GetLookupRootPage(orderByFieldMetadata.Field.FieldName);
            // Note that in here we have to check for the *lowest* value of the field, if there are multiple terms
            // for the field, so we always order by the smallest value (regardless of the ascending / descending structure)
            switch (orderByField.OrderingType)
            {
                case OrderByFieldType.Long:
                    long l = long.MaxValue;
                    while (reader.FindNext(fieldRootPage))
                    {
                        l = long.Min(l, reader.CurrentLong);
                    }
                    result.AddLongOrderByField(l);
                    break;
                case OrderByFieldType.Double:
                    double d = double.MaxValue;
                    while (reader.FindNext(fieldRootPage))
                    {
                        d = double.Min(d, reader.CurrentDouble);
                    }
                    result.AddDoubleOrderByField(d);
                    break;
                case OrderByFieldType.Distance:
                {
                    double m = double.MaxValue;
                    while (reader.FindNextSpatial(fieldRootPage))
                    {
                        var coordinates = (reader.Latitude, reader.Longitude);
                     
                        var distance = SpatialUtils.GetGeoDistance(in coordinates, (orderByFieldMetadata.Point.X, orderByFieldMetadata.Point.Y), orderByFieldMetadata.Round, orderByFieldMetadata.Units);
                        m = double.Min(m, distance);
                    }

                    result.AddDoubleOrderByField(m);
                    break;
                }
                default:
                {
                    string m = null;
                    while (reader.FindNext(fieldRootPage))
                    {
                        if (reader.IsNull || reader.IsNonExisting)
                        {
                            m = null;
                            continue;
                        }
                        // we are allocating managed string to make things easier, if this show up in profiling
                        // we can do the comparisons using CompactKeys
                        var stringValue = Encoding.UTF8.GetString(reader.Current.Decoded());
                        m ??= stringValue;
                        if (string.CompareOrdinal(m, stringValue) < 0)
                            m = stringValue;
                    }
                    result.AddStringOrderByField(m switch
                    {
                         Constants.EmptyString => string.Empty,
                         _ => m
                    });
                    break;
                }
            }
        }

        return result;
    }

    private static bool IsSameOrderType(MatchCompareFieldType coraxOrderField, OrderByFieldType queryOrderField)
    {
        bool result = (coraxOrderField, queryOrderField) switch
        {
            (MatchCompareFieldType.Random, OrderByFieldType.Random) => true,
            (MatchCompareFieldType.Alphanumeric, OrderByFieldType.AlphaNumeric) => true,
            (MatchCompareFieldType.Score, OrderByFieldType.Score) => true,
            (MatchCompareFieldType.Integer, OrderByFieldType.Long) => true,
            (MatchCompareFieldType.Floating, OrderByFieldType.Double) => true,
            (MatchCompareFieldType.Spatial, OrderByFieldType.Distance) => true,
            (MatchCompareFieldType.Sequence, OrderByFieldType.String) => true,
            (MatchCompareFieldType.Sequence, OrderByFieldType.Implicit) => true,
            (MatchCompareFieldType.Integer, OrderByFieldType.Implicit) => true,
            _ => false
        };

        return result;
    }
    
    internal override void AssertCanOrderByScoreAutomaticallyWhenBoostingIsInvolved() => throw new NotSupportedInShardingException($"Ordering by score is not supported in sharding. You received this exception because your index has boosting, and we attempted to sort the results since the configuration `{RavenConfiguration.GetKey(i => i.Indexing.OrderByScoreAutomaticallyWhenBoostingIsInvolved)}` is enabled.");
}
