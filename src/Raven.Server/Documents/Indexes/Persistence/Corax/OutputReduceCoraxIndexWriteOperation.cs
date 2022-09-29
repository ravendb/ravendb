using System.Diagnostics;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Sparrow.Json;
using Sparrow.Logging;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public class OutputReduceCoraxIndexWriteOperation : CoraxIndexWriteOperation
{
    private readonly OutputReduceIndexWriteOperationScope<CoraxIndexWriteOperation> _outputScope;

    public OutputReduceCoraxIndexWriteOperation(MapReduceIndex index, Transaction writeTransaction, CoraxDocumentConverterBase converter, Logger logger,
        JsonOperationContext indexContext) : base(index,
        writeTransaction, converter, logger)
    {
        Debug.Assert(index.OutputReduceToCollection != null);
        _outputScope = new(index, writeTransaction, indexContext, this);
    }

    public override void Commit(IndexingStatsScope stats) => _outputScope.Commit(stats);

    public override void IndexDocument(LazyStringValue key, LazyStringValue sourceDocumentId, object document, IndexingStatsScope stats,
        JsonOperationContext indexContext) => _outputScope.IndexDocument(key, sourceDocumentId, document, stats, indexContext);

    public override void Delete(LazyStringValue key, IndexingStatsScope stats) => _outputScope.Delete(key, stats);

    public override void DeleteReduceResult(LazyStringValue reduceKeyHash, IndexingStatsScope stats) => _outputScope.DeleteReduceResult(reduceKeyHash, stats);

    public override void Dispose()
    {
        base.Dispose();
        _outputScope.Dispose();
    }
}
