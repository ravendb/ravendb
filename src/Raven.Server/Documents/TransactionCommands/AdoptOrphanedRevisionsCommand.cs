using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.TransactionCommands
{
    internal class AdoptOrphanedRevisionsCommand : TransactionOperationsMerger.MergedTransactionCommand
    {
        private readonly RevisionsStorage _revisionsStorage;
        private readonly List<string> _ids;
        private readonly AdoptOrphanedResult _result;
        private readonly OperationCancelToken _token;

        public AdoptOrphanedRevisionsCommand(
            RevisionsStorage revisionsStorage,
            List<string> ids,
            AdoptOrphanedResult result,
            OperationCancelToken token)
        {
            _revisionsStorage = revisionsStorage;
            _ids = ids;
            _result = result;
            _token = token;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            foreach (var id in _ids)
            {
                _token.ThrowIfCancellationRequested();
                _result.AdoptedRevisionsCount += _revisionsStorage.AdoptOrphanedFor(context, id);
            }

            return _ids.Count;
        }

        public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto<TTransaction>(TransactionOperationContext<TTransaction> context)
        {
            return new AdoptOrphanedRevisionsCommandDto(_revisionsStorage, _ids);
        }

        private class AdoptOrphanedRevisionsCommandDto : TransactionOperationsMerger.IReplayableCommandDto<AdoptOrphanedRevisionsCommand>
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
                return new AdoptOrphanedRevisionsCommand(_revisionsStorage, _ids, new AdoptOrphanedResult(), OperationCancelToken.None);
            }
        }


        public class AdoptOrphanedResult : OperationResult
        {
            public long AdoptedRevisionsCount { get; set; }

            public override DynamicJsonValue ToJson()
            {
                var json = base.ToJson();
                json[nameof(AdoptedRevisionsCount)] = AdoptedRevisionsCount;
                return json;
            }
        }
    }

}
