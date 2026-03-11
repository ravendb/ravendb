using System;
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

        public BatchTrackChangesCommand(Dictionary<string, string> trackedEntities)
        {
            _trackedEntities = trackedEntities;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            foreach (Document document in context.DocumentDatabase.DocumentsStorage.GetDocuments(context, _trackedEntities.Keys, start: 0, take: int.MaxValue, DocumentFields.Id | DocumentFields.ChangeVector, returnNonExists: true))
            {
                var expected = _trackedEntities[document.Id];
                if (expected == null)
                    continue;

                ChangeVector current = context.GetChangeVector(document.ChangeVector);

                if (current.IsEqual(context.GetChangeVector(expected)) == false)
                {
                    throw new ConcurrencyException($"Document '{document.Id}' has been modified since it was loaded. The expected change vector '{expected}' does not match the current change vector '{document.ChangeVector}'.")
                    {
                        Id = document.Id,
                        ActualChangeVector = document.ChangeVector,
                        ExpectedChangeVector = expected
                    };
                }
            }

            return 1;
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
        {
            return new BatchTrackChangesCommandDto
            {
                TrackedEntities = _trackedEntities
            };
        }

        public sealed class BatchTrackChangesCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>>
        {
            public Dictionary<string, string> TrackedEntities { get; set; }

            public MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction> ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                return new BatchTrackChangesCommand(TrackedEntities);
            }
        }

        public static Dictionary<string, string> Parse(BlittableJsonReaderObject trackedEntities)
        {
            var dic = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

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
