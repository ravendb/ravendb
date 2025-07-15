using System;
using Raven.Client;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using PutOperationResults = Raven.Server.Documents.DocumentsStorage.PutOperationResults;

namespace Raven.Server.Documents.Handlers.AI.Agents
{
    internal class PutChatCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
    {
        private string _id;
        private ConversationDocument _conversation;
        private BlittableJsonReaderObject _conversationDoc;
        private BlittableJsonReaderObject _historyDoc;
        private readonly LazyStringValue _expectedChangeVector;
        private DocumentDatabase _database;
        private AiAgentConfiguration _configuration;
        public (PutOperationResults Conversation, PutOperationResults History) PutResult;

        private const string AiAgentConversationHistoryIdAddition = "History";

        public PutChatCommand(string conversationId, ConversationDocument conversation, BlittableJsonReaderObject history, LazyStringValue changeVector, AiAgentConfiguration configuration, DocumentDatabase database)
        {
            _id = conversationId;
            _conversation = conversation;
            _historyDoc = history;
            _expectedChangeVector = changeVector;
            _database = database;
            _configuration = configuration;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            _id = _database.DocumentsStorage.DocumentPut.BuildDocumentId(_id, _database.DocumentsStorage.GenerateNextEtag(), out _);

            PutOperationResults putHistoryResult = default;
            if (_historyDoc != null)
            {
                var historyId = $"{_id}{_database.IdentityPartsSeparator}{AiAgentConversationHistoryIdAddition}{_database.IdentityPartsSeparator}";
                historyId = _database.DocumentsStorage.DocumentPut.BuildDocumentId(historyId, _database.DocumentsStorage.GenerateNextEtag(), out _);
                putHistoryResult = _database.DocumentsStorage.Put(context, historyId, _expectedChangeVector, _historyDoc);
                _conversation.HistoryDocuments.Add(putHistoryResult.Id);
            }

            _conversationDoc = _conversation.ToBlittable(context, _configuration, _configuration.Persistence?.Expires);
            var putResult = _database.DocumentsStorage.Put(context, _id, _expectedChangeVector, _conversationDoc);
            PutResult = (putResult, putHistoryResult);

            return 1;
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
        {
            return new PutChatCommandDto(_id, _conversation, _historyDoc, _expectedChangeVector, _configuration, _database);
        }

        public class PutChatCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, PutChatCommand>
        {
            private string _id;
            private ConversationDocument _conversation;
            private BlittableJsonReaderObject _historyDoc;
            private readonly LazyStringValue _expectedChangeVector;
            private DocumentDatabase _database;
            private AiAgentConfiguration _configuration;

            public PutChatCommandDto(string conversationId, ConversationDocument conversation, BlittableJsonReaderObject history, LazyStringValue changeVector, AiAgentConfiguration configuration, DocumentDatabase database)
            {
                _id = conversationId;
                _conversation = conversation;
                _historyDoc = history;
                _expectedChangeVector = changeVector;
                _database = database;
                _configuration = configuration;
            }

            public PutChatCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                return new PutChatCommand(_id, _conversation, _historyDoc, _expectedChangeVector, _configuration, _database);
            }
        }
    }

}
