using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.TransactionCommands
{
    internal abstract class RevisionsScanningOperationCommand<TOperationResult> : TransactionOperationsMerger.MergedTransactionCommand
        where TOperationResult : OperationResult
    {
        public bool MoreWork;

        protected readonly RevisionsStorage _revisionsStorage;

        protected readonly List<string> _ids;

        protected readonly OperationCancelToken _token;

        protected TOperationResult _result;

        public RevisionsScanningOperationCommand(
            RevisionsStorage revisionsStorage,
            List<string> ids,
            TOperationResult result,
            OperationCancelToken token)
        {
            _revisionsStorage = revisionsStorage;
            _ids = ids;
            _result = result;
            _token = token;
        }
    }
}
