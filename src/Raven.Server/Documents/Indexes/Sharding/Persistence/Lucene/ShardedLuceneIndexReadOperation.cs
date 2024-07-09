using Lucene.Net.Search;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Config;
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
    public ShardedLuceneIndexReadOperation(Index index, LuceneVoronDirectory directory,
        QueryBuilderFactories queryBuilderFactories, Transaction readTransaction, IndexQueryServerSide query) : base(index, directory, 
        queryBuilderFactories, readTransaction, query)
    {
    }

    protected override QueryResult CreateQueryResult(Document doc, CreateQueryResultParameters parameters, ref bool markedAsSkipped, Reference<long> skippedResults, ref int returnedResults)
    {
        var result = base.CreateQueryResult(doc, parameters, ref markedAsSkipped, skippedResults, ref returnedResults);

        var query = parameters.Query;

        if (result.Result != null && query.ReturnOptions != null)
        {
            if (query.ReturnOptions.AddOrderByFieldsMetadata)
            {
                if (_index.Type.IsMapReduce() == false) // for a map-reduce index the returned results already have fields that are used for sorting
                    result.Result = AddOrderByFields(result.Result, query, parameters.ScoreDoc.Doc);
            }

            if (query.ReturnOptions.AddDataHashMetadata) 
                result.Result = result.Result.EnsureDataHashInQueryResultMetadata();
        }

        return result;
    }

    private ShardedQueryResultDocument AddOrderByFields(Document queryResult, IndexQueryServerSide query, int doc)
    {
        var result = ShardedQueryResultDocument.From(queryResult);

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
                    string stringValue = _searcher.IndexReader.GetStringValueFor(field.OrderByName, doc, _state);

                    switch (stringValue)
                    {
                        case Constants.Documents.Indexing.Fields.NullValue:
                            stringValue = null;
                            break;

                        case Constants.Documents.Indexing.Fields.EmptyString:
                            stringValue = string.Empty;
                            break;
                    }

                    result.AddStringOrderByField(stringValue);
                    break;
            }
        }

        return result;
    }
    
    internal override void AssertCanOrderByScoreAutomaticallyWhenBoostingIsInvolved() => throw new NotSupportedInShardingException($"Ordering by score is not supported in sharding. You received this exception because your index has boosting, and we attempted to sort the results since the configuration `{RavenConfiguration.GetKey(i => i.Indexing.OrderByScoreAutomaticallyWhenBoostingIsInvolved)}` is enabled.");
}
