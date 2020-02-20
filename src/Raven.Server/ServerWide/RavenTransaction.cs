using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Voron.Impl;

namespace Raven.Server.ServerWide
{
    public class RavenTransaction : IDisposable
    {
        private List<CompareExchangeChange> _compareExchangeNotifications;

        public Transaction InnerTransaction;
        protected readonly ClusterChanges _clusterChanges;

        public RavenTransaction(Transaction transaction, ClusterChanges clusterChanges)
        {
            InnerTransaction = transaction;
            _clusterChanges = clusterChanges;
        }

        public void Commit()
        {
            InnerTransaction.Commit();
        }

        public void EndAsyncCommit()
        {
            InnerTransaction.EndAsyncCommit();
        }

        public bool Disposed;

        public virtual void Dispose()
        {
            if (Disposed)
                return;

            Disposed = true;

            var committed = InnerTransaction.LowLevelTransaction.Committed;

            InnerTransaction?.Dispose();
            InnerTransaction = null;

            if (committed)
                AfterCommit();
        }

        public void AddAfterCommitNotification(CompareExchangeChange change)
        {
            Debug.Assert(_clusterChanges != null, "_clusterChanges != null");

            if (_compareExchangeNotifications == null)
                _compareExchangeNotifications = new List<CompareExchangeChange>();
            _compareExchangeNotifications.Add(change);
        }

        protected bool ShouldRaiseNotifications()
        {
            return _compareExchangeNotifications != null;
        }

        protected virtual void RaiseNotifications()
        {
            if (_compareExchangeNotifications?.Count > 0)
            {
                foreach (var notification in _compareExchangeNotifications)
                {
                    _clusterChanges.RaiseNotifications(notification);
                }
            }
        }

        private void AfterCommit()
        {
            if (ShouldRaiseNotifications() == false)
                return;

            ThreadPool.QueueUserWorkItem(state => ((RavenTransaction)state).RaiseNotifications(), this);
        }
    }
}
