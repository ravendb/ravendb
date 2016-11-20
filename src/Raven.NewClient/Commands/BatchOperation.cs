using System;
using System.Collections.Generic;
using System.Text;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Document.Commands;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.NewClient.Client.Commands
{
    public class BatchOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<LoadOperation>("Raven.NewClient.Client");

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

        private List<object> _entities;
        private int _deferredCommandsCount;

        public BatchCommand CreateRequest()
        {
            var result = _session.PrepareForSaveChanges();
            _session.IncrementRequestCount();
            LogBatch();

            _entities = result.Entities;
            _deferredCommandsCount = result.DeferredCommandsCount;

            return new BatchCommand()
            {
                Commands = result.Commands,
                Context = _session.Context
            };
        }

        public void SetResult(BatchResult result)
        {
            for (var i = _deferredCommandsCount; i < result.Results.Length; i++)
            {
                var batchResult = result.Results[i] as BlittableJsonReaderObject;
                if (batchResult == null)
                    throw new ArgumentNullException();

                string methodType;
                batchResult.TryGet("Method", out methodType);

                if (methodType != "PUT")
                    continue;

                var entity = _entities[i - _deferredCommandsCount];
                DocumentInfo documentInfo;

                if (_session.DocumentsByEntity.TryGetValue(entity, out documentInfo) == false)
                    continue;

                string key;
                long? etag;
                BlittableJsonReaderObject metadata;
                if (batchResult.TryGet("Metadata", out metadata))
                    documentInfo.Metadata = metadata;
                if (batchResult.TryGet("Etag", out etag))
                    documentInfo.ETag = etag;
                if (batchResult.TryGet("Key", out key))
                {
                    documentInfo.Id = key;
                    _session.DocumentsById[key] = documentInfo;
                }

                var afterStoreEventArgs = new AfterStoreEventArgs(_session, documentInfo.Id, documentInfo.Entity);
                _session.OnAfterStoreInvoke(afterStoreEventArgs);
            }
        }
    }
}