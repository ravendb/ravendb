// -----------------------------------------------------------------------
//  <copyright file="EvictItemsFromCacheBasedOnChanges.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Client.Util;

namespace Raven.Client.Documents.Changes
{
    internal class EvictItemsFromCacheBasedOnChanges : IObserver<DocumentChange>, IObserver<IndexChange>, IObserver<AggressiveCacheUpdate>, IDisposable
    {
        private readonly string _databaseName;
        private readonly DatabaseChanges _changes;
        private IDisposable _documentsSubscription;
        private IDisposable _indexesSubscription;
        private readonly RequestExecutor _requestExecutor;
        private readonly Task _taskConnected;
        private IDisposable _aggressiveCachingSubscription;

        public EvictItemsFromCacheBasedOnChanges(DocumentStore store, string databaseName)
        {
            _databaseName = databaseName;
            _requestExecutor = store.GetRequestExecutor(databaseName);
            _changes = new DatabaseChanges(_requestExecutor, databaseName, onDispose: null,nodeTag: null);
            _taskConnected = EnsureConnectedInternalAsync();
        }

        private async Task EnsureConnectedInternalAsync()
        {
            await _changes.EnsureConnectedNow().ConfigureAwait(false);
            var changesSupportedFeatures = await _changes.GetSupportedFeatures().ConfigureAwait(false);
            if (changesSupportedFeatures.AggressiveCachingChange)
            {
                var forAggressiveCachingChanges = _changes.ForAggressiveCaching();
                _aggressiveCachingSubscription = forAggressiveCachingChanges.Subscribe(this);
                await forAggressiveCachingChanges.EnsureSubscribedNow().ConfigureAwait(false);
            }
            else
            {
                var docSub = _changes.ForAllDocuments();
                _documentsSubscription = docSub.Subscribe(this);
                var indexSub = _changes.ForAllIndexes();
                _indexesSubscription = indexSub.Subscribe(this);
                await docSub.EnsureSubscribedNow().ConfigureAwait(false);
                await indexSub.EnsureSubscribedNow().ConfigureAwait(false);
            }
        }

        public void EnsureConnected()
        {
            AsyncHelpers.RunSync(EnsureConnectedAsync);
        }

        public Task EnsureConnectedAsync() => _taskConnected;

        public void OnNext(DocumentChange change)
        {
            if (change.Type == DocumentChangeTypes.Put || change.Type == DocumentChangeTypes.Delete)
            {
                Interlocked.Increment(ref _requestExecutor.Cache.Generation);
            }
        }

        public void OnNext(IndexChange change)
        {
            if (change.Type == IndexChangeTypes.BatchCompleted || change.Type == IndexChangeTypes.IndexRemoved)
            {
                Interlocked.Increment(ref _requestExecutor.Cache.Generation);
            }
        }

        public void OnError(Exception error)
        {
        }

        public void OnNext(AggressiveCacheUpdate value)
        {
            Interlocked.Increment(ref _requestExecutor.Cache.Generation);
        }

        public void OnCompleted()
        {
        }

        public void Dispose()
        {
            using (_changes)
            {
                _documentsSubscription?.Dispose();
                _indexesSubscription?.Dispose();
                _aggressiveCachingSubscription?.Dispose();
            }
        }
    }
}
