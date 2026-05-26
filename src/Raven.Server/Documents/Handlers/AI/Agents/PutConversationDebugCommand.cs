using System.Collections.Generic;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.AI.Agents
{
    internal sealed class PutConversationDebugCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
    {
        private readonly List<AiDebugTrace> _traces;
        private readonly ConversationDocument _conversation;
        private readonly DocumentDatabase _database;

        public PutConversationDebugCommand(List<AiDebugTrace> traces, ConversationDocument conversation, DocumentDatabase database)
        {
            _traces = traces;
            _conversation = conversation;
            _database = database;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            var sep = _database.IdentityPartsSeparator;
            var idPrefix = $"{_conversation.Id}{sep}{AiDebugTrace.TraceSegment}{sep}";
            foreach (var trace in _traces)
            {
                var doc = trace.ToBlittable(context, _conversation, _conversation.Expires);
                _database.DocumentsStorage.Put(context, idPrefix, expectedChangeVector: null, doc,
                    nonPersistentFlags: NonPersistentDocumentFlags.SkipSchemaValidation);
            }

            return 1;
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
        {
            return new PutConversationDebugCommandDto(_traces, _conversation, _database);
        }

        public class PutConversationDebugCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, PutConversationDebugCommand>
        {
            private readonly List<AiDebugTrace> _traces;
            private readonly ConversationDocument _conversation;
            private readonly DocumentDatabase _database;

            public PutConversationDebugCommandDto(List<AiDebugTrace> traces, ConversationDocument conversation, DocumentDatabase database)
            {
                _traces = traces;
                _conversation = conversation;
                _database = database;
            }

            public PutConversationDebugCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                return new PutConversationDebugCommand(_traces, _conversation, _database);
            }
        }
    }
}
