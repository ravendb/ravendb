using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.MapReduce.Static.Sharding;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Sharding;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Queries;

public class ShardedMapReduceQueryResultsMerger
{
    protected readonly List<BlittableJsonReaderObject> CurrentResults;
    private readonly ShardedDatabaseContext.ShardedIndexesContext _indexesContext;
    private readonly string _indexName;
    private readonly bool _isAutoMapReduceQuery;
    protected readonly TransactionOperationContext Context;

    public ShardedMapReduceQueryResultsMerger(List<BlittableJsonReaderObject> currentResults, ShardedDatabaseContext.ShardedIndexesContext indexesContext, string indexName, bool isAutoMapReduceQuery, TransactionOperationContext context)
    {
        CurrentResults = currentResults;
        _indexesContext = indexesContext;
        _indexName = indexName;
        _isAutoMapReduceQuery = isAutoMapReduceQuery;
        Context = context;
    }

    public List<BlittableJsonReaderObject> Merge()
    {
        var index = _indexesContext.GetIndex(_indexName);
        if (index == null)
            IndexDoesNotExistException.ThrowFor(_indexName);

        if (index.Type.IsAutoMapReduce())
        {
            var autoMapReduceIndexDefinition = (AutoMapReduceIndexDefinition)index.Definition;
            AggregationResult aggregateOn = AggregateForAutoMapReduce(autoMapReduceIndexDefinition);
            return aggregateOn.GetOutputsToStore().ToList();
        }

        if (_isAutoMapReduceQuery)
            throw new InvalidOperationException($"Failed to get {_indexName} index for the reduce part in the orchestrator");

        if (index.Type.IsStaticMapReduce() == false)
            throw new InvalidOperationException($"Index '{_indexName}' is not a map-reduce index");

        return AggregateForStaticMapReduce(index);
    }

    protected virtual List<BlittableJsonReaderObject> AggregateForStaticMapReduce(IndexInformationHolder index)
    {
        using (CurrentIndexingScope.Current = new OrchestratorIndexingScope())
        {
            var compiled = ((StaticIndexInformationHolder)index).Compiled;

            var reducingFunc = compiled.Reduce;
            var blittableToDynamicWrapper = new ReduceMapResultsOfStaticIndex.DynamicIterationOfAggregationBatchWrapper();
            blittableToDynamicWrapper.InitializeForEnumeration(CurrentResults);

            var results = new List<object>();
            IPropertyAccessor propertyAccessor = null;
            foreach (var output in reducingFunc(blittableToDynamicWrapper))
            {
                propertyAccessor ??= PropertyAccessor.Create(output.GetType(), output);
                results.Add(output);
            }

            if (propertyAccessor == null)
                return new List<BlittableJsonReaderObject>(0);

            var objects = new ShardedAggregatedAnonymousObjects(results, propertyAccessor, Context);
            return objects.GetOutputsToStore().ToList();
        }
    }

    protected virtual AggregationResult AggregateForAutoMapReduce(AutoMapReduceIndexDefinition indexDefinition)
    {
        BlittableJsonReaderObject currentlyProcessedResult = null;
        return ReduceMapResultsOfAutoIndex.Aggregator.AggregateOn(CurrentResults, indexDefinition, Context, null, ref currentlyProcessedResult, CancellationToken.None);
    }
}
