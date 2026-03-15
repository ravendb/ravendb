using System.Collections.Generic;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Voron.Util.RateLimiting;

namespace Raven.Server.Documents.TransactionMerger.Commands
{
    internal abstract class RevisionsScanningOperationCommand<TOperationResult> : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
        where TOperationResult : OperationResult
    {
        public bool MoreWork;

        public bool NeedWait;

        protected readonly RevisionsStorage _revisionsStorage;

        protected readonly List<string> _ids;

        protected readonly OperationCancelToken _token;

        protected readonly RateGate _rateGate;

        protected TOperationResult _result;

        public RevisionsScanningOperationCommand(
            RevisionsStorage revisionsStorage,
            List<string> ids,
            TOperationResult result,
            OperationCancelToken token,
            RateGate rateGate = null)
        {
            _revisionsStorage = revisionsStorage;
            _ids = ids;
            _result = result;
            _token = token;
            _rateGate = rateGate;
        }
    }
}
