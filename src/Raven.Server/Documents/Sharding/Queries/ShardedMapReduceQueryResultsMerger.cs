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
using Raven.Server.Logging;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Sharding.Queries;

public class ShardedMapReduceQueryResultsMerger
{
    private static readonly ShardedAutoMapReduceIndexResultsAggregator Aggregator = new();

    protected readonly List<BlittableJsonReaderObject> CurrentResults;
    private readonly ShardedDatabaseContext.ShardedIndexesContext _indexesContext;
    private readonly string _indexName;
    private readonly bool _isAutoMapReduceQuery;
    protected readonly TransactionOperationContext Context;
    protected readonly CancellationToken Token;
    private readonly RavenLogger _logger;

    public ShardedMapReduceQueryResultsMerger(List<BlittableJsonReaderObject> currentResults, ShardedDatabaseContext.ShardedIndexesContext indexesContext, string indexName, bool isAutoMapReduceQuery, TransactionOperationContext context, CancellationToken token)
    {
        CurrentResults = currentResults;
        _indexesContext = indexesContext;
        _indexName = indexName;
        _isAutoMapReduceQuery = isAutoMapReduceQuery;
        Context = context;
        Token = token;
        _logger = RavenLogManager.Instance.GetLoggerForDatabase<ShardedMapReduceQueryResultsMerger>(indexesContext.DatabaseContext);
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
            throw new InvalidOperationException($"Failed to get '{_indexName}' index for the reduce part in the orchestrator");

        if (index.Type.IsStaticMapReduce() == false)
            throw new InvalidOperationException($"Index '{_indexName}' is not a map-reduce index");

        return AggregateForStaticMapReduce(index);
    }

    protected virtual List<BlittableJsonReaderObject> AggregateForStaticMapReduce(IndexInformationHolder index)
    {
        using (var unmanagedBuffersPool = new UnmanagedBuffersPoolWithLowMemoryHandling(_logger, $"Sharded//Indexes//{index.Name}"))
        using (CurrentIndexingScope.Current = new OrchestratorIndexingScope(Context, unmanagedBuffersPool))
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

            var objects = CreateShardedAggregatedAnonymousObjects(results, propertyAccessor, index.Configuration.IndexMissingFieldsAsNull == false);
            return objects.GetOutputsToStore().ToList();
        }
    }

    protected virtual AggregatedAnonymousObjects CreateShardedAggregatedAnonymousObjects(List<object> results, IPropertyAccessor propertyAccessor, bool skipImplicitNullInOutput = false)
        => new ShardedAggregatedAnonymousObjects(results, propertyAccessor, Context, skipImplicitNullInOutput);

    internal virtual AggregationResult AggregateForAutoMapReduce(AutoMapReduceIndexDefinition indexDefinition)
    {
        BlittableJsonReaderObject currentlyProcessedResult = null;
        return Aggregator.AggregateOn(CurrentResults, indexDefinition, Context, null, ref currentlyProcessedResult, Token);
    }
}
