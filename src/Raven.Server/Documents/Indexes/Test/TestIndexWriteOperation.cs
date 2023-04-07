using System;
using System.Threading;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Indexes.Test;

public class TestIndexWriteOperation : IndexWriteOperationBase
{
    private readonly IndexWriteOperationBase _inner;
    private readonly TestIndexRun _testIndexRun;

    public TestIndexWriteOperation(IndexWriteOperationBase writerRetriever, Index index) : base(index, LoggingSource.Instance.GetLogger<LuceneIndexWriteOperation>(index._indexStorage.DocumentDatabase.Name))
    {
        if (index.IsTestRun == false) 
            throw new InvalidOperationException($"{nameof(TestIndexWriteOperation)} should only be used for test indexes.");

        _inner = writerRetriever;
        _testIndexRun = index.TestRun;
    }

    public override void Dispose()
    {
        _inner?.Dispose();
    }

    public override void Commit(IndexingStatsScope stats)
    {
        _inner.Commit(stats);
    }

    public override void Optimize(CancellationToken token)
    {
        _inner.Optimize(token);
    }

    public override void UpdateDocument(string keyFieldName, LazyStringValue key, LazyStringValue sourceDocumentId, object document, IndexingStatsScope stats,
        JsonOperationContext indexContext)
    {
        if (_index.Type.IsMap())
            _testIndexRun.AddMapResult(document);
        else
            _testIndexRun.AddReduceResult(document);
        
        _inner.UpdateDocument(keyFieldName, key, sourceDocumentId, document, stats, indexContext);
    }

    public override void IndexDocument(LazyStringValue key, LazyStringValue sourceDocumentId, object document, IndexingStatsScope stats, JsonOperationContext indexContext)
    {
        if (_index.Type.IsMap())
            _testIndexRun.AddMapResult(document);
        else
            _testIndexRun.AddReduceResult(document);
            
        _inner.IndexDocument(key, sourceDocumentId, document, stats, indexContext);
    }

    public override long EntriesCount()
    {
        return _inner.EntriesCount();
    }

    public override (long RamSizeInBytes, long FilesAllocationsInBytes) GetAllocations()
    {
        return _inner.GetAllocations();
    }

    public override void Delete(LazyStringValue key, IndexingStatsScope stats)
    {
        _inner.Delete(key, stats);
    }

    public override void DeleteBySourceDocument(LazyStringValue sourceDocumentId, IndexingStatsScope stats)
    {
        _inner.DeleteBySourceDocument(sourceDocumentId, stats);
    }

    public override void DeleteReduceResult(LazyStringValue reduceKeyHash, IndexingStatsScope stats)
    {
        _inner.DeleteReduceResult(reduceKeyHash, stats);
    }
    
}
