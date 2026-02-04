using System.Collections.Generic;
using Raven.Client.Exceptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.TransactionMerger.Commands
{
    public sealed class BatchTrackChangesCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
    {
        private readonly Dictionary<string, string> _trackedEntities;
        private readonly DocumentDatabase _database;

        public BatchTrackChangesCommand(Dictionary<string, string> trackedEntities, DocumentDatabase database)
        {
            _trackedEntities = trackedEntities;
            _database = database;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            foreach (Document document in _database.DocumentsStorage.GetDocuments(context, _trackedEntities.Keys, start: 0, take: int.MaxValue, DocumentFields.Id | DocumentFields.ChangeVector))
            {
                //TODO: egor use CVs
                ChangeVector docCv = context.GetChangeVector(document.ChangeVector);

                var expectedCv = _trackedEntities[document.Id];

                docCv.IsEqual(context.GetChangeVector(expectedCv));

                if (expectedCv != document.ChangeVector)
                {
                    throw new ConcurrencyException("Document change vector mismatch")
                    {
                        Id = document.Id,
                        ActualChangeVector = document.ChangeVector,
                        ExpectedChangeVector = expectedCv
                    };
                }
            }

            return 1;
        }


        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
        {
            return new BatchTrackChangesCommandDto();
        }

        public sealed class BatchTrackChangesCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>>
        {

            public MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction> ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                return null;
                // return new BatchTrackChangesCommand();
            }
        }

        public static Dictionary<string, string> Parse(BlittableJsonReaderObject trackedEntities)
        {
            var dic = new Dictionary<string, string>();

            foreach (var key in trackedEntities.GetPropertyNames())
            {
                if (trackedEntities.TryGet(key, out string value) == false)
                    continue;

                dic[key] = value;
            }

            return dic;
        }
    }
}
