using System;
using System.Threading;
using Raven.Abstractions.Logging;
using Raven.Server.Documents;

namespace Raven.Server.ReplicationUtil
{
    public abstract class BaseReplicationExecuter : IDisposable
    {
        protected readonly ILog _log;

        protected readonly DocumentDatabase _database;
        protected Thread _replicationThread;
        protected bool _disposed;

        protected readonly CancellationTokenSource _cancellationTokenSource;
        public CancellationToken CancellationToken => _cancellationTokenSource.Token;
        public readonly ManualResetEventSlim WaitForChanges = new ManualResetEventSlim();

        public abstract string ReplicationUniqueName { get; }

        protected BaseReplicationExecuter(DocumentDatabase database)
        {
            _log = LogManager.GetLogger(GetType());
            _database = database;
            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
        }

        public void Start()
        {
            if (_replicationThread != null)
                return;

            _replicationThread = new Thread(ExecuteReplicationLoop)
            {
                Name = $"Replication thread, {ReplicationUniqueName}",
                IsBackground = true
            };

            _replicationThread.Start();
        }

        protected abstract void ExecuteReplicationOnce();

        /// <summary>
        /// returns true if there are items left to replicate, and false 
        /// if all items were replicated and the replication needs to go to sleep
        /// </summary>
        protected abstract bool ShouldWaitForChanges();

        private void ExecuteReplicationLoop()
        {
            while (_cancellationTokenSource.IsCancellationRequested == false)
            {
                if (_log.IsDebugEnabled)
                    _log.Debug($"Starting replication for '{ReplicationUniqueName}'.");

                WaitForChanges.Reset();

                try
                {
                    _cancellationTokenSource.Token.ThrowIfCancellationRequested();

                    ExecuteReplicationOnce();

                    if (_log.IsDebugEnabled)
                        _log.Debug($"Finished replication for '{ReplicationUniqueName}'.");
                }
                catch (OutOfMemoryException oome)
                {
                    _log.WarnException($"Out of memory occured for '{ReplicationUniqueName}'.", oome);
                    // TODO [ppekrol] GC?
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception e)
                {
                    _log.WarnException($"Exception occured for '{ReplicationUniqueName}'.", e);
                }

                if (!ShouldWaitForChanges())
                    continue;

                try
                {
                    WaitForChanges.Wait(_cancellationTokenSource.Token);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }

        public virtual void Dispose()
        {
            try
            {
                _cancellationTokenSource.Cancel();					
            }
            catch (ObjectDisposedException)
            {
                //precaution, should not happen
                _log.Warn("ObjectDisposedException thrown during replication executer disposal, should not happen. Something is wrong here.");
            }
            catch (AggregateException e)
            {
                _log.Error("Error during replication executer disposal, most likely it is a bug.",e);
            }
            finally
            {
                _disposed = true;
            }
        }
    }
}