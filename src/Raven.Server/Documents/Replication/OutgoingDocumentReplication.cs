using System;
using System.Diagnostics;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Server.ReplicationUtil;
using Raven.Server.ServerWide.Context;
using Sparrow;

namespace Raven.Server.Documents.Replication
{
    public class OutgoingDocumentReplication : IDisposable
    {
        private readonly DocumentDatabase _database;
        protected long _lastSentEtag;
        private readonly DocumentReplicationTransport _transport;
        private readonly ILog _log = LogManager.GetLogger(typeof (OutgoingDocumentReplication));
        private readonly DocumentsOperationContext _context;
        private bool _isInitialized;

        private readonly Thread _replicationThread;
        private readonly CancellationTokenSource _cancellationTokenSource;

        public readonly AsyncManualResetEvent _waitForChanges;
        private readonly string _replicationUniqueName;

        public OutgoingDocumentReplication(DocumentDatabase database, 
            ReplicationDestination destination,
            DocumentReplicationTransport transport = null)
        {
            _database = database;
            _database.Notifications.OnDocumentChange += HandleDocumentChange;
            _lastSentEtag = -1;
            _database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context);
            
            _transport = transport ?? new DocumentReplicationTransport(
                destination.Url,
                _database.DbId,
                _database.Name,
                destination.Database,
                _database.DatabaseShutdown,
                _context);

            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
            _waitForChanges = new AsyncManualResetEvent(_cancellationTokenSource.Token);

            _replicationUniqueName = $"Outgoing Replication Thread <{destination.Url} -> {destination.Database}>";
            _replicationThread = new Thread(() => ReplicateDocumentsAsync().Wait())
            {
                IsBackground = true,
                Name = _replicationUniqueName
            };

        }

        //TODO : add parameter to notification that would indicate that the document
        //was received from a replication propagation and not from a user
        private void HandleDocumentChange(DocumentChangeNotification notification)
        {
            _waitForChanges.SetByAsyncCompletion();
        }

        private async Task ReplicateDocumentsAsync()
        {
            while (_cancellationTokenSource.IsCancellationRequested == false)
            {
                if (_log.IsDebugEnabled)
                    _log.Debug($"Starting replication for '{_replicationUniqueName}'.");

                _waitForChanges.Reset();

                try
                {
                    _cancellationTokenSource.Token.ThrowIfCancellationRequested();
                    await ExecuteReplicationOnce();

                    if (_log.IsDebugEnabled)
                        _log.Debug($"Finished replication for '{_replicationUniqueName}'.");
                }
                catch (OutOfMemoryException oome)
                {
                    _log.WarnException($"Out of memory occured for '{_replicationUniqueName}'.", oome);
                    // TODO [ppekrol] GC?
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception e)
                {
                    _log.WarnException($"Exception occured for '{_replicationUniqueName}'.", e);
                }

                if (HasMoreDocumentsToSend)
                    continue;

                try
                {
                    //if this returns false, this means canceled token is activated                    
                    if (await _waitForChanges.WaitAsync() == false)
                        //thus, if code reaches here, cancellation token source has "cancel" requested
                        return;
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }

        public bool HasMoreDocumentsToSend
        {
            get
            {
                using(_context.OpenReadTransaction())
                    return DocumentsStorage.ReadLastEtag(
                        _context.Transaction.InnerTransaction) < _lastSentEtag;
            }
        }

        private async Task ExecuteReplicationOnce()
        {
            Debug.Assert(_isInitialized);

            //just for shorter code
            var documentStorage = _database.DocumentsStorage;
            using (_context.OpenReadTransaction())
            {
                //TODO: make replication batch size configurable
                //also, perhaps there should be timers/heuristics
                //that would dynamically resize batch size
                var replicationBatch =
                    documentStorage
                        .GetDocumentsAfter(_context, _lastSentEtag, 0, 1024)
                        .Where(x => !x.Key.ToString().StartsWith("Raven/"))
                        .ToList();
                        //the filtering here will need to be reworked -> it is not efficient
                        //TODO: do not forget to make version of GetDocumentsAfter with a prefix filter	
                        //alternatively 1 -> create efficient StartsWith() for LazyString				
                        //alternatively 2 -> create a "filter system" that would abstract the logic -> what documents 
                        //should and should not be replicated
                if (replicationBatch.Count == 0)
                    return;
                try
                {
                    _lastSentEtag = await _transport.SendDocumentBatchAsync(replicationBatch)
                                                    .WithCancellation(_cancellationTokenSource.Token);
                }
                catch (WebSocketException e)
                {
                    _log.Warn("Sending document replication batch is interrupted. This is not necessarily an issue. Reason: " + e);
                }
                catch (Exception e)
                {
                    _log.Error("Sending document replication batch has failed. Reason: " + e);
                }
            }
        }

        public async Task InitializeAsync()
        {
            await _transport.EnsureConnectionAsync();
            _lastSentEtag = await _transport.GetLastEtag();
            _replicationThread.Start();
            _isInitialized = true;
        }

        public void Dispose()
        {
            _context.Dispose();
            _database.Notifications.OnDocumentChange -= HandleDocumentChange;
            try
            {
                _replicationThread.Join(1000);
            }
            catch (ThreadStateException)
            {
            }
        }
    }
}
