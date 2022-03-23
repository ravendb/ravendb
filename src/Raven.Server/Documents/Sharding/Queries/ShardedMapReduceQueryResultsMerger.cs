using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.MapReduce.Static.Sharding;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Queries;

public class ShardedMapReduceQueryResultsMerger
{
    private readonly List<BlittableJsonReaderObject> _currentResults;
    private readonly ShardedDatabaseContext.ShardedIndexesCache _indexesCache;
    private readonly string _indexName;
    private readonly bool _isAutoMapReduceQuery;
    private readonly TransactionOperationContext _context;

    public ShardedMapReduceQueryResultsMerger(List<BlittableJsonReaderObject> currentResults, ShardedDatabaseContext.ShardedIndexesCache indexesCache, string indexName, bool isAutoMapReduceQuery, TransactionOperationContext context)
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

        if (_indexesCache.TryGetAutoMapReduceIndexDefinition(_indexName, out var autoIndexDefinition))
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
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "add a test for this");
            return new List<BlittableJsonReaderObject>();
        }

        var objects = new ShardedAggregatedAnonymousObjects(results, propertyAccessor, _context);
        return objects.GetOutputsToStore().ToList();
    }
}
