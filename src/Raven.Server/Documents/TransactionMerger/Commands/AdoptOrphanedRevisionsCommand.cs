using System.Collections.Generic;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.TransactionMerger.Commands;
internal class AdoptOrphanedRevisionsCommand : RevisionsScanningOperationCommand<AdoptOrphanedRevisionsResult>
{
    public AdoptOrphanedRevisionsCommand(
        RevisionsStorage revisionsStorage,
        List<string> ids,
        AdoptOrphanedRevisionsResult result,
        OperationCancelToken token) : base(revisionsStorage, ids, result, token)
    {
        MoreWork = false;
    }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        foreach (var id in _ids)
        {
            _token.ThrowIfCancellationRequested();
            if (_revisionsStorage.AdoptOrphanedFor(context, id))
                _result.AdoptedCount++;
        }

        return _ids.Count;
    }

    public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
    {
        return new AdoptOrphanedRevisionsCommandDto(_revisionsStorage, _ids);
    }

    private sealed class AdoptOrphanedRevisionsCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, AdoptOrphanedRevisionsCommand>
    {
        private readonly RevisionsStorage _revisionsStorage;
        private readonly List<string> _ids;
        
        public AdoptOrphanedRevisionsCommandDto(RevisionsStorage revisionsStorage, List<string> ids)
        {
            _revisionsStorage = revisionsStorage;
            _ids = ids;
        }
        
        public AdoptOrphanedRevisionsCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            return new AdoptOrphanedRevisionsCommand(_revisionsStorage, _ids, new AdoptOrphanedRevisionsResult(), OperationCancelToken.None);
        }
    }

}

