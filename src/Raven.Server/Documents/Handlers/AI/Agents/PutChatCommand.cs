using System;
using Raven.Client;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using PutOperationResults = Raven.Server.Documents.DocumentsStorage.PutOperationResults;

namespace Raven.Server.Documents.Handlers.AI.Agents
{
    internal class PutChatCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>, IDisposable
    {
        private string _id;
        private ConversationDocument _conversation;
        private ConversationDocument _history;
        private readonly LazyStringValue _expectedChangeVector;
        private DocumentDatabase _database;
        private AiAgentConfiguration _configuration;
        public (PutOperationResults Conversation, PutOperationResults History) PutResult;

        private BlittableJsonReaderObject _conversationDoc;
        private BlittableJsonReaderObject _historyDoc;

        public PutChatCommand(string conversationId, ConversationDocument conversation, ConversationDocument history, LazyStringValue changeVector, AiAgentConfiguration configuration, DocumentDatabase database)
        {
            _id = conversationId;
            _conversation = conversation;
            _history = history;
            _expectedChangeVector = changeVector;
            _database = database;
            _configuration = configuration;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            if (_id?.EndsWith(_database.IdentityPartsSeparator) == true)
                _id = MergedPutCommand.GenerateNonConflictingId(_database, _id);

            PutOperationResults putHistoryResult = default;
            if (_history != null)
            {
                var historyId = $"{_id}{_database.IdentityPartsSeparator}{Constants.Documents.Collections.AiAgentConversationHistoryIdAddition}{_database.IdentityPartsSeparator}";
                MergedPutCommand.GenerateNonConflictingId(_database, historyId);
                _historyDoc = _history.ToBlittable(context, _configuration);
                putHistoryResult = _database.DocumentsStorage.Put(context, historyId, _expectedChangeVector, _historyDoc);
                _conversation.HistoryDocuments.Add(putHistoryResult.Id);
            }

            _conversationDoc = _conversation.ToBlittable(context, _configuration);
            var putResult = _database.DocumentsStorage.Put(context, _id, _expectedChangeVector, _conversationDoc);
            PutResult = (putResult, putHistoryResult);

            return 1;
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
        {
            return new PutChatCommandDto(_id, _conversation, _history, _expectedChangeVector, _configuration, _database);
        }

        public void Dispose()
        {
            _historyDoc?.Dispose();
            _conversationDoc?.Dispose();
        }

        public class PutChatCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, PutChatCommand>
        {
            private string _id;
            private ConversationDocument _conversation;
            private ConversationDocument _history;
            private readonly LazyStringValue _expectedChangeVector;
            private DocumentDatabase _database;
            private AiAgentConfiguration _configuration;

            public PutChatCommandDto(string conversationId, ConversationDocument conversation, ConversationDocument history, LazyStringValue changeVector, AiAgentConfiguration configuration, DocumentDatabase database)
            {
                _id = conversationId;
                _conversation = conversation;
                _history = history;
                _expectedChangeVector = changeVector;
                _database = database;
                _configuration = configuration;
            }

            public PutChatCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                return new PutChatCommand(_id, _conversation, _history, _expectedChangeVector, _configuration, _database);
            }
        }
    }

}
