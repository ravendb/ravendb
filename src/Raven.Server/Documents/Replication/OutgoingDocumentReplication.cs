using System;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Server.ReplicationUtil;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Replication
{
    public class OutgoingDocumentReplication : IDisposable
    {
        private const string SystemDocumentPrefix = "Raven/";
        private readonly DocumentDatabase _database;
        protected long _lastSentEtag;
        private bool _hasMoreDocumentsToSend;
        private readonly DocumentReplicationTransport _transport;
        private readonly ILog _log = LogManager.GetLogger(typeof (OutgoingDocumentReplication));

        //private readonly SemaphoreSlim _replicationSemaphore = new SemaphoreSlim(0,1);

        public OutgoingDocumentReplication(
            DocumentDatabase database,             
            DocumentReplicationTransport transport)
        {
            _database = database;           
            _transport = transport;
            _lastSentEtag = -1;
        }

        public bool HasMoreDocumentsToSend => _hasMoreDocumentsToSend;

        public void ExecuteReplicationOnce()
        {
            AsyncHelpers.RunSync(() => _transport.EnsureConnectionAsync());
            if (_lastSentEtag == -1)
                _lastSentEtag = _transport.GetLastEtag();
            var lastSendEtag = _lastSentEtag;

            //just for shorter code
            var documentStorage = _database.DocumentsStorage;
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
                    _hasMoreDocumentsToSend = true;
                    return;
                }

                try
                {
                    AsyncHelpers.RunSync(() => _transport.SendDocumentBatchAsync(replicationBatch));
                }
                catch (WebSocketException e)
                {
                    _log.Warn("Sending document replication batch is interrupted. This is not necessarily an issue. Reason: " +
                              e);
                    return;
                }
                catch (Exception e)
                {
                    _log.Error("Sending document replication batch has failed. Reason: " + e);
                    return;
                }

                _lastSentEtag = replicationBatch.Max(x => x.Etag);
                var lastExistingEtag = DocumentsStorage.ReadLastEtag(context.Transaction.InnerTransaction);
                _hasMoreDocumentsToSend = lastExistingEtag <= _lastSentEtag;
            }
        }

        public void Dispose()
        {
            //_replicationSemaphore.Dispose();
        }
    }
}
