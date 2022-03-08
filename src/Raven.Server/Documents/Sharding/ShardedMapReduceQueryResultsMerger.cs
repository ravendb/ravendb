using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding;

public class ShardedMapReduceQueryResultsMerger
{
    private static readonly DynamicJsonValue DummyDynamicJsonValue = new();
    private readonly List<BlittableJsonReaderObject> _currentResults;
    private readonly ShardedIndexesCache _indexesCache;
    private readonly string _indexName;
    private readonly bool _isAutoMapReduceQuery;
    private readonly TransactionOperationContext _context;

    public ShardedMapReduceQueryResultsMerger(List<BlittableJsonReaderObject> currentResults, ShardedIndexesCache indexesCache, string indexName, bool isAutoMapReduceQuery, TransactionOperationContext context)
    {
        _currentResults = currentResults;
        _indexesCache = indexesCache;
        _indexName = indexName;
        _isAutoMapReduceQuery = isAutoMapReduceQuery;
        _context = context;
    }

    public List<BlittableJsonReaderObject> Merge()
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "Make sure that we have the auto map index in the orchestrator");

        if (_indexesCache.TryGetAutoIndexDefinition(_indexName, out var autoIndexDefinition))
        {
            var autoMapReduceIndexDefinition = (AutoMapReduceIndexDefinition)IndexStore.CreateAutoDefinition(autoIndexDefinition, IndexDeploymentMode.Parallel);
            var aggegation = ReduceMapResultsOfAutoIndex.AggregateOn(_currentResults, autoMapReduceIndexDefinition, _context, null, CancellationToken.None);
            return aggegation.GetOutputsToStore().ToList();
        }

        if (_isAutoMapReduceQuery)
        {
            throw new InvalidOperationException($"Failed to get {_indexName} index for the reduce part in the orchestrator");
        }

        var compiled = _indexesCache.GetCompiledMapReduceIndex(_indexName, _context);
        if (compiled == null)
            throw new IndexDoesNotExistException($"Index {_indexName} doesn't exist");

        var reducingFunc = compiled.Reduce;
        var blittableToDynamicWrapper = new ReduceMapResultsOfStaticIndex.DynamicIterationOfAggregationBatchWrapper();
        blittableToDynamicWrapper.InitializeForEnumeration(_currentResults);

        var results = new List<object>();
        IPropertyAccessor propertyAccessor = null;
        foreach (var output in reducingFunc(blittableToDynamicWrapper))
        {
            propertyAccessor ??= PropertyAccessor.Create(output.GetType(), output);
            results.Add(output);
        }

        if (propertyAccessor == null)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "test this");
            return new List<BlittableJsonReaderObject>();
        }

        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "handle metadata merge, score, distance");
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "test this");

        var objects = new AggregatedAnonymousObjects(results, propertyAccessor, _context, djv =>
        {
            djv[Constants.Documents.Metadata.Key] = DummyDynamicJsonValue;
        });

        return objects.GetOutputsToStore().ToList();
    }
}
