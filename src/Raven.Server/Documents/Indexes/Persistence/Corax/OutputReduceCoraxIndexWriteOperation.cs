using System.Diagnostics;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Sparrow.Json;
using Sparrow.Logging;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public sealed class OutputReduceCoraxIndexWriteOperation : CoraxIndexWriteOperation
{
    private readonly OutputReduceIndexWriteOperationScope<OutputReduceCoraxIndexWriteOperation> _outputScope;

    public OutputReduceCoraxIndexWriteOperation(MapReduceIndex index, Transaction writeTransaction, CoraxDocumentConverterBase converter, RavenLogger logger,
        JsonOperationContext indexContext) : base(index, writeTransaction, converter, logger)
    {
        Debug.Assert(index.OutputReduceToCollection != null);
        _outputScope = new(index, writeTransaction, indexContext, this);
    }
    
    public override void Commit(IndexingStatsScope stats)
    {
        if (_outputScope.IsActive)
            base.Commit(stats);
        else
            _outputScope.Commit(stats);
    }

    public override void IndexDocument(LazyStringValue key, LazyStringValue sourceDocumentId, object document, IndexingStatsScope stats,
        JsonOperationContext indexContext)
    {
        if (_outputScope.IsActive)
            base.IndexDocument(key, sourceDocumentId, document, stats, indexContext);
        else
            _outputScope.IndexDocument(key, sourceDocumentId, document, stats, indexContext);
    }

    public override void Delete(LazyStringValue key, IndexingStatsScope stats)
    {
        if (_outputScope.IsActive)
            base.Delete(key, stats);
        else
            _outputScope.Delete(key, stats);
    }
    
    public override void DeleteByPrefix(LazyStringValue key, IndexingStatsScope stats)
    {
         if (_outputScope.IsActive)
             base.DeleteByPrefix(key, stats);
         else
             _outputScope.Delete(key, stats);
    }

    public override void UpdateDocument(LazyStringValue key, LazyStringValue sourceDocumentId, object document, IndexingStatsScope stats, JsonOperationContext indexContext)
    {
        if (_outputScope.IsActive)
            base.UpdateDocument(key, sourceDocumentId, document, stats, indexContext);
        else
            _outputScope.UpdateDocument(key, sourceDocumentId, document, stats, indexContext);
    }

    public override void DeleteReduceResult(LazyStringValue reduceKeyHash, IndexingStatsScope stats)
    {
        if (_outputScope.IsActive)
            base.DeleteReduceResult(reduceKeyHash, stats);
        else
            _outputScope.DeleteReduceResult(reduceKeyHash, stats);
    }
    
    public override void Dispose()
    {
        base.Dispose();
        _outputScope.Dispose();
    }
}
