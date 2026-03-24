using System.Collections.Generic;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.AI.Agents
{
    internal class PutConversationCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
    {
        public DocumentsStorage.PutOperationResults PutResult;

        private BlittableJsonReaderObject _conversationDoc;
        private readonly ConversationDocument _conversation;
        private readonly List<BlittableJsonReaderObject> _historyDocs;
        private readonly LazyStringValue _expectedChangeVector;
        private readonly DocumentDatabase _database;
        private readonly AiAgentConfiguration _configuration;
        public MergedBatchCommand Attachments { get; set; }
        private const string AiAgentConversationHistoryIdPrefix = "ConversationHistory";

        public PutConversationCommand(ConversationDocument conversation, List<BlittableJsonReaderObject> history, LazyStringValue changeVector, AiAgentConfiguration configuration, DocumentDatabase database)
        {
            _conversation = conversation;
            _historyDocs = history;
            _expectedChangeVector = changeVector;
            _database = database;
            _configuration = configuration;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            if (_historyDocs != null)
            {
                foreach (var historyDoc in _historyDocs)
                {
                    var historyId = _database.DocumentsStorage.DocumentPut.BuildDocumentId($"{AiAgentConversationHistoryIdPrefix}{_database.IdentityPartsSeparator}", _database.DocumentsStorage.GenerateNextEtag(), out _);
                    historyId = $"{historyId}${_conversation.Id}";

                    var putHistoryResult = _database.DocumentsStorage.Put(context, historyId, null, historyDoc, nonPersistentFlags: NonPersistentDocumentFlags.SkipSchemaValidation);
                    _conversation.LinkedConversations.Add(putHistoryResult.Id);
                }
            }

            _conversationDoc = _conversation.ToBlittable(context);
            PutResult = _database.DocumentsStorage.Put(context, _conversation.Id, _expectedChangeVector, _conversationDoc, nonPersistentFlags: NonPersistentDocumentFlags.SkipSchemaValidation);

            if (Attachments is not null)
            {
                Attachments.ExecuteDirectly(context);
                var d = _database.DocumentsStorage.GetDocumentOrTombstone(context, PutResult.Id, DocumentFields.ChangeVector);// Attachments will change the document change vector; re-read to return the final CV.
                PutResult.ChangeVector = d.Document.ChangeVector;
            }

            return 1;
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
        {
            return new PutChatCommandDto(_conversation, _historyDocs, _expectedChangeVector, _configuration, _database);
        }

        public class PutChatCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, PutConversationCommand>
        {
            private ConversationDocument _conversation;
            private List<BlittableJsonReaderObject> _historyDocs;
            private readonly LazyStringValue _expectedChangeVector;
            private DocumentDatabase _database;
            private AiAgentConfiguration _configuration;

            public PutChatCommandDto(ConversationDocument conversation, List<BlittableJsonReaderObject> history, LazyStringValue changeVector, AiAgentConfiguration configuration, DocumentDatabase database)
            {
                _conversation = conversation;
                _historyDocs = history;
                _expectedChangeVector = changeVector;
                _database = database;
                _configuration = configuration;
            }

            public PutConversationCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                return new PutConversationCommand(_conversation, _historyDocs, _expectedChangeVector, _configuration, _database);
            }
        }
    }

}
