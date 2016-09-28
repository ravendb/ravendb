using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Document;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.SessionOperations.Commands;
using Raven.Json.Linq;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Client.Documents.SessionOperations
{
    public class BatchOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<LoadOperation>("Raven.Client");

        public InMemoryDocumentSessionOperations.SaveChangesData Data;

        public BatchOperation(InMemoryDocumentSessionOperations session)
        {
            _session = session;
        }

        protected void LogBatch()
        {
            if (_logger.IsInfoEnabled)
            {
                var sb = new StringBuilder()
                    .AppendFormat("Saving {0} changes to {1}", Data.Commands.Count, _session.StoreIdentifier)
                    .AppendLine();
                foreach (var commandData in Data.Commands)
                {
                    sb.AppendFormat("\t{0} {1}", commandData["Method"], commandData["Key"]).AppendLine();
                }
                _logger.Info(sb.ToString());
            }
        }

        private List<object> entities;
        private int DeferredCommandsCount;

        public BatchCommand CreateRequest()
        {
            _session.IncrementRequestCount();
            LogBatch();

            var result = _session.PrepareForSaveChanges();
            entities = result.Entities;
            DeferredCommandsCount = result.DeferredCommandsCount;
            return new BatchCommand()
            {
                Commands = result.Commands,
                Context = _session.Context
            };

        }

        public void SetResult(BatchResult result)
        {
            //TODO - work in grogress
            for (var i = DeferredCommandsCount; i < result.Results.Length; i++)
            {
                var batchResult = result.Results[i] as BlittableJsonReaderObject;
                string methodType;
                batchResult.TryGet("Method", out methodType);

                if (methodType != "PUT")
                    continue;

                var entity = entities[i - DeferredCommandsCount];
                InMemoryDocumentSessionOperations.DocumentInfo documentInfo;

                if (_session.DocumentsByEntity.TryGetValue(entity, out documentInfo) == false)
                    continue;

                string Key;

                BlittableJsonReaderObject metadata;
                 batchResult.TryGet("Metadata", out metadata);
                documentInfo.Metadata = metadata;
                batchResult.TryGet("Key", out Key);
                _session.DocumentsById[Key] = documentInfo;
                /*documentMetadata.ETag = batchResult.Etag;
                documentMetadata.Key = batchResult.Key;
                documentMetadata.OriginalMetadata = (RavenJObject)batchResult.Metadata.CloneToken();
                documentMetadata.Metadata = batchResult.Metadata;
                documentMetadata.OriginalValue = EntityToJson.ConvertEntityToJson(documentMetadata.Key, entity, documentMetadata.Metadata);

                GenerateEntityIdOnTheClient.TrySetIdentity(entity, batchResult.Key);

                foreach (var documentStoreListener in theListeners.StoreListeners)
                {
                    documentStoreListener.AfterStore(batchResult.Key, entity, batchResult.Metadata);
                }#1#*/
            }
        }
    }
}