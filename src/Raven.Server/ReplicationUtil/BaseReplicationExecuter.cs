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

        public abstract string Name { get; }

        protected BaseReplicationExecuter(DocumentDatabase database)
        {
            _log = LogManager.GetLogger(GetType());
            _database = database;
            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
        }

        public void Start()
        {
            _replicationThread = new Thread(ExecuteReplicationLoop)
            {
                Name = $"Replication thread, {Name}",
                IsBackground = true
            };

            _replicationThread.Start();
        }

        protected abstract void ExecuteReplicationOnce();

        private void ExecuteReplicationLoop()
        {
            while (_cancellationTokenSource.IsCancellationRequested == false)
            {
                if (_log.IsDebugEnabled)
                    _log.Debug($"Starting replication for '{Name}'.");

                WaitForChanges.Reset();

                try
                {
                    _cancellationTokenSource.Token.ThrowIfCancellationRequested();

                    ExecuteReplicationOnce();

                    if (_log.IsDebugEnabled)
                        _log.Debug($"Finished replication for '{Name}'.");
                }
                catch (OutOfMemoryException oome)
                {
                    _log.WarnException($"Out of memory occured for '{Name}'.", oome);
                    // TODO [ppekrol] GC?
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception e)
                {
                    _log.WarnException($"Exception occured for '{Name}'.", e);
                }

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