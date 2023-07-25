using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Indexes.Test;

public class TestIndexRun
{
    private class CollectionIterationStats
    {
        public int CountOfReturnedItems;
        public bool Completed;
    }
    
    public List<BlittableJsonReaderObject> MapResults;
    public List<BlittableJsonReaderObject> ReduceResults;
    public ManualResetEventSlim BatchCompleted;
    private readonly JsonOperationContext _context;
    private Dictionary<string, CollectionIterationStats> _collectionTracker;
    private readonly int _docsToProcessPerCollection;
    private int _collectionsCount;

    public TestIndexRun(JsonOperationContext context, int docsToProcessPerCollection, int numberOfCollections)
    {
        _context = context;
        _docsToProcessPerCollection = docsToProcessPerCollection;
        _collectionsCount = numberOfCollections;
        _collectionTracker = new Dictionary<string, CollectionIterationStats>();
        MapResults = new List<BlittableJsonReaderObject>();
        ReduceResults = new List<BlittableJsonReaderObject>();
        BatchCompleted = new ManualResetEventSlim();
    }

    public TestIndexWriteOperation CreateIndexWriteOperationWrapper(IndexWriteOperationBase writer, Index index)
    {
        return new TestIndexWriteOperation(writer, index);
    }

    public void HandleCanContinueBatch(Index.CanContinueBatchResult result, string collection)
    {
        Debug.Assert(result is Index.CanContinueBatchResult.False or Index.CanContinueBatchResult.RenewTransaction, $"{result} is Index.CanContinueBatchResult.False or Index.CanContinueBatchResult.RenewTransaction");

        if (_collectionTracker.TryGetValue(collection, out var stats) == false)
            return;

        stats.CountOfReturnedItems--;
        stats.Completed = stats.CountOfReturnedItems > _docsToProcessPerCollection;
    }

    public IEnumerable<IndexItem> CreateEnumeratorWrapper(IEnumerable<IndexItem> enumerator, string collection)
    {
        if (_collectionTracker.TryGetValue(collection, out var stats) == false)
            _collectionTracker[collection] = stats = new CollectionIterationStats();
        
        foreach (IndexItem item in enumerator)
        {
            if (++stats.CountOfReturnedItems > _docsToProcessPerCollection)
            {
                stats.Completed = true;
                yield break;
            }
            
            yield return item;
        }

        stats.Completed = true;
    }

    public void WaitForProcessingOfSampleDocs(TimeSpan waitForProcessingTimespan)
    {
        var sw = new Stopwatch();
        
        sw.Start();
        
        while (sw.Elapsed < waitForProcessingTimespan)
        {
            BatchCompleted.Wait();

            BatchCompleted.Reset();
            
            if (_collectionTracker == null)
                continue;

            if (_collectionTracker.Count < _collectionsCount)
                continue;

            var missing = false;

            foreach (var tracker in _collectionTracker)
            {
                if (tracker.Value.Completed == false)
                    missing = true;
            }

            if (missing == false)
                return;
        }
    }

    public void AddMapResult(object result)
    {
        var item = ConvertToBlittable(result);
            
        MapResults.Add(item);
    }

    public void AddMapResult(BlittableJsonReaderObject mapResult, string collection)
    {
        BlittableJsonReaderObject result = mapResult.Clone(_context);
            
        MapResults.Add(result);
    }
        
    public void AddReduceResult(object result)
    {
        var item = ConvertToBlittable(result);
            
        ReduceResults.Add(item);
    }
        
    private BlittableJsonReaderObject ConvertToBlittable(object result)
    {
        var djv = new DynamicJsonValue();
        IPropertyAccessor propertyAccessor = PropertyAccessor.Create(result.GetType(), result);

        foreach (var property in propertyAccessor.GetProperties(result))
        {
            var value = property.Value;
            djv[property.Key] = TypeConverter.ToBlittableSupportedType(value, context: _context);
        }
            
        return _context.ReadObject(djv, "test-index-result");
    }
}
