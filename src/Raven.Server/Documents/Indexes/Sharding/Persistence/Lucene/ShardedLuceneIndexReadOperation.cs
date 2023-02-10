using Lucene.Net.Search;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Sharding.Queries;
using Raven.Server.Indexing;
using Raven.Server.Utils;
using Sparrow.Utils;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Sharding.Persistence.Lucene;

public sealed class ShardedLuceneIndexReadOperation : LuceneIndexReadOperation
{
    public ShardedLuceneIndexReadOperation(Index index, LuceneVoronDirectory directory, LuceneIndexSearcherHolder searcherHolder,
        QueryBuilderFactories queryBuilderFactories, Transaction readTransaction, IndexQueryServerSide query) : base(index, directory, searcherHolder,
        queryBuilderFactories, readTransaction, query)
    {
    }

    protected override QueryResult CreateQueryResult(Document doc, CreateQueryResultParameters parameters, ref bool markedAsSkipped, Reference<long> skippedResults, ref int returnedResults)
    {
        var result = base.CreateQueryResult(doc, parameters, ref markedAsSkipped, skippedResults, ref returnedResults);
        
        if (result.Result != null && ShardedQueryResultDocument.ShouldAddShardingSpecificMetadata(parameters.Query, _index.Type, out var shouldAdd))
        {
            var shardedResult = ShardedQueryResultDocument.From(result.Result);

            if (shouldAdd.OrderByFields)
                shardedResult = AddOrderByFields(shardedResult, parameters.Query, parameters.ScoreDoc.Doc);

            if (shouldAdd.DistinctDataHash)
                shardedResult.DistinctDataHash = shardedResult.DataHash;

            result.Result = shardedResult;
        }

        return result;
    }

    private ShardedQueryResultDocument AddOrderByFields(ShardedQueryResultDocument result, IndexQueryServerSide query, int doc)
    {
        foreach (var field in query.Metadata.OrderBy)
        {
            switch (field.OrderingType)
            {
                case OrderByFieldType.Long:
                    result.AddLongOrderByField(_searcher.IndexReader.GetLongValueFor(field.OrderByName, FieldCache_Fields.NUMERIC_UTILS_LONG_PARSER, doc, _state));
                    break;
                case OrderByFieldType.Double:
                    result.AddDoubleOrderByField(_searcher.IndexReader.GetDoubleValueFor(field.OrderByName, FieldCache_Fields.NUMERIC_UTILS_DOUBLE_PARSER, doc, _state));
                    break;
                case OrderByFieldType.Random:
                    // we order by random when merging results from shards
                    break;
                case OrderByFieldType.Distance:
                    result.AddDoubleOrderByField(result.Distance.Value.Distance);
                    break;
                case OrderByFieldType.Score:
                    DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal, "RavenDB-13927 Order by score");

                    throw new NotSupportedInShardingException("Ordering by score is not supported in sharding");
                default:
                    result.AddStringOrderByField(_searcher.IndexReader.GetStringValueFor(field.OrderByName, doc, _state));
                    break;
            }
        }

        return result;
    }
}
