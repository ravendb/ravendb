using System.Collections.Generic;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Voron.Util.RateLimiting;

namespace Raven.Server.Documents.TransactionMerger.Commands;

internal sealed class EnforceRevisionConfigurationCommand : RevisionsScanningOperationCommand<EnforceConfigurationResult>
{
    private readonly bool _includeForceCreatedRevisionsOnDeleteInCaseOfNoConfiguration;

    public EnforceRevisionConfigurationCommand(
        RevisionsStorage revisionsStorage,
        List<string> ids,
        EnforceConfigurationResult result,
        bool includeForceCreated,
        OperationCancelToken token,
        RateGate rateGate = null) : base(revisionsStorage, ids, result, token, rateGate)
    {
        _includeForceCreatedRevisionsOnDeleteInCaseOfNoConfiguration = includeForceCreated;
    }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        MoreWork = false;
        NeedWait = false;
        for (int i = _ids.Count - 1; i >= 0; i--)
        {
            _token.ThrowIfCancellationRequested();

            if (_rateGate != null && _rateGate.WaitToProceed(0) == false)
            {
                NeedWait = true;
                break;
            }

            var moreWork = false;
            _result.RemovedRevisions += (int)_revisionsStorage.EnforceConfigurationFor(context, _ids[i], _includeForceCreatedRevisionsOnDeleteInCaseOfNoConfiguration == false, ref moreWork);
            if (moreWork == false)
                _ids.RemoveAt(i);
            else
                MoreWork = true;
        }

        return 1;
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
