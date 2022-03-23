using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
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
    private readonly ShardedDatabaseContext.ShardedIndexesContext _indexesContext;
    private readonly string _indexName;
    private readonly bool _isAutoMapReduceQuery;
    private readonly TransactionOperationContext _context;

    public ShardedMapReduceQueryResultsMerger(List<BlittableJsonReaderObject> currentResults, ShardedDatabaseContext.ShardedIndexesContext indexesContext, string indexName, bool isAutoMapReduceQuery, TransactionOperationContext context)
    {
        _currentResults = currentResults;
        _indexesContext = indexesContext;
        _indexName = indexName;
        _isAutoMapReduceQuery = isAutoMapReduceQuery;
        _context = context;
    }

    public List<BlittableJsonReaderObject> Merge()
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "Make sure that we have the auto map index in the orchestrator");

        var index = _indexesContext.GetIndex(_indexName);
        if (index != null && index.Type.IsAutoMapReduce())
        {
            var autoMapReduceIndexDefinition = (AutoMapReduceIndexDefinition)index.Definition;
            var aggregateOn = ReduceMapResultsOfAutoIndex.AggregateOn(_currentResults, autoMapReduceIndexDefinition, _context, null, CancellationToken.None);
            return aggregateOn.GetOutputsToStore().ToList();
        }

        if (_isAutoMapReduceQuery)
        {
            throw new InvalidOperationException($"Failed to get {_indexName} index for the reduce part in the orchestrator");
        }

        var compiled = _indexesContext.GetCompiledMapReduceIndex(_indexName, _context);
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
