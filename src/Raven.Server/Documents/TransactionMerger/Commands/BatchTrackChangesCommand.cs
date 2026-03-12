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
                {
                    // we don't care
                    continue;
                }

                if (document.ChangeVector == string.Empty && expected == string.Empty)
                {
                    // this document doesn't exist, and it wasn't existing when we loaded it, so we are good
                    continue;
                }

                ChangeVector current = context.GetChangeVector(document.ChangeVector);

                if (current.IsEqual(context.GetChangeVector(expected)) == false)
                {

                    var expectedStr = expected == string.Empty ? "string.Empty" : expected;
                    var currentStr = document.ChangeVector == null ? "NULL" : document.ChangeVector == string.Empty ? "string.Empty" : document.ChangeVector;
                    throw new ConcurrencyException($"Document '{document.Id}' has been modified since it was loaded. The expected change vector '{expectedStr}' does not match the current change vector '{currentStr}'.")
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
