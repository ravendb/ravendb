using System;
using System.Linq;
using System.Net.WebSockets;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Server.ReplicationUtil;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Replication
{
    public class OutgoingDocumentReplication
    {
        private const string SystemDocumentPrefix = "Raven/";
        private readonly DocumentDatabase _database;
        protected long _lastSentEtag;
        private bool _shouldWaitForChanges;
        private readonly DocumentReplicationTransport _transport;
        private readonly ILog _log = LogManager.GetLogger(typeof (OutgoingDocumentReplication));

        public OutgoingDocumentReplication(
            DocumentDatabase database, 
            long lastSentEtag, 
            DocumentReplicationTransport transport)
        {
            _database = database;
            _lastSentEtag = lastSentEtag;
            _transport = transport;
        }

        public bool ShouldWaitForChanges => _shouldWaitForChanges;

        public void ExecuteReplicationOnce()
        {
            var lastSendEtag = _lastSentEtag;

            //just for shorter code
            var documentStorage = _database.DocumentsStorage;
            //TODO: handle here properly last etag
            //either add here negotiation for the etag, 
            //or add etag tracking
            DocumentsOperationContext context;
            using (documentStorage.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                //TODO: make replication batch size configurable
                //also, perhaps there should be timers/heuristics
                //that would dynamically resize batch size
                var replicationBatch =
                    documentStorage
                        .GetDocumentsAfter(context, lastSendEtag, 0, 1024)
                        .Where(x => !x.Key.ToString().StartsWith(SystemDocumentPrefix))
                        .ToArray();

                if (replicationBatch.Length == 0)
                {
                    _shouldWaitForChanges = true;
                    return;
                }

                //TODO : consider changing SendDocumentBatchAsync to sync version
                try
                {
                    AsyncHelpers.RunSync(() =>
                        _transport.SendDocumentBatchAsync(replicationBatch, context));
                }
                catch (WebSocketException e)
                {
                    _log.Warn("Sending document replication batch is interrupted. This is not necessarily an issue. Reason: " + e);
                    return;
                }
                catch (Exception e)
                {
                    _log.Error("Sending document replication batch has failed. Reason: " + e);
                    return;
                }

                _lastSentEtag = replicationBatch.Max(x => x.Etag);
                var lastExistingEtag = DocumentsStorage.ReadLastEtag(context.Transaction.InnerTransaction);
                _shouldWaitForChanges = lastExistingEtag <= _lastSentEtag;
            }
        }
    }
}
