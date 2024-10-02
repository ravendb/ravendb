using System.Collections.Generic;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.TransactionMerger.Commands;

internal sealed class EnforceRevisionConfigurationCommand : RevisionsScanningOperationCommand<EnforceConfigurationResult>
{
    private readonly bool _includeForceCreatedRevisionsOnDeleteInCaseOfNoConfiguration;

    public EnforceRevisionConfigurationCommand(
        RevisionsStorage revisionsStorage,
        List<string> ids,
        EnforceConfigurationResult result,
        bool includeForceCreated,
        OperationCancelToken token) : base(revisionsStorage, ids, result, token)
    {
        _includeForceCreatedRevisionsOnDeleteInCaseOfNoConfiguration = includeForceCreated;
    }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        MoreWork = false;
        foreach (var id in _ids)
        {
            _token.ThrowIfCancellationRequested();
            _result.RemovedRevisions += (int)_revisionsStorage.EnforceConfigurationFor(context, id, _includeForceCreatedRevisionsOnDeleteInCaseOfNoConfiguration == false, ref MoreWork);
        }

        return _ids.Count;
    }

    public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
    {
        return new EnforceRevisionConfigurationCommandDto(_revisionsStorage, _ids, _includeForceCreatedRevisionsOnDeleteInCaseOfNoConfiguration);
    }

    private sealed class EnforceRevisionConfigurationCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, EnforceRevisionConfigurationCommand>
    {
        private readonly RevisionsStorage _revisionsStorage;
        private readonly List<string> _ids;
        private readonly bool _includeForceCreated;

        public EnforceRevisionConfigurationCommandDto(RevisionsStorage revisionsStorage, List<string> ids, bool includeForceCreated)
        {
            _revisionsStorage = revisionsStorage;
            _ids = ids;
            _includeForceCreated = includeForceCreated;
        }

        public EnforceRevisionConfigurationCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            return new EnforceRevisionConfigurationCommand(_revisionsStorage, _ids, new EnforceConfigurationResult(), _includeForceCreated, OperationCancelToken.None);
        }
    }
}
