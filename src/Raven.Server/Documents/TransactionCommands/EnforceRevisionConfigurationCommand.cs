using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.TransactionCommands
{
    internal class EnforceRevisionConfigurationCommand : RevisionsScanningOperationCommand<EnforceConfigurationResult>
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
            for (int i = _ids.Count - 1; i >= 0; i--)
            {
                _token.ThrowIfCancellationRequested();
                _result.RemovedRevisions += (int)_revisionsStorage.EnforceConfigurationFor(context, _ids[i], _includeForceCreatedRevisionsOnDeleteInCaseOfNoConfiguration == false, out var moreWork);
                if (moreWork == false)
                    _ids.RemoveAt(i);
                else
                    MoreWork = true;
            }

            return 1;
        }
    
        public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto<TTransaction>(TransactionOperationContext<TTransaction> context)
        {
            return new EnforceRevisionConfigurationCommandDto(_revisionsStorage, _ids, _includeForceCreatedRevisionsOnDeleteInCaseOfNoConfiguration);
        }
    
        private class EnforceRevisionConfigurationCommandDto : TransactionOperationsMerger.IReplayableCommandDto<EnforceRevisionConfigurationCommand>
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
                return new EnforceRevisionConfigurationCommand(_revisionsStorage, _ids,  new EnforceConfigurationResult(), _includeForceCreated, OperationCancelToken.None);
            }
        }
    }
}
