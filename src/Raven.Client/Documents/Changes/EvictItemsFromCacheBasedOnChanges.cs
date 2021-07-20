// -----------------------------------------------------------------------
//  <copyright file="EvictItemsFromCacheBasedOnChanges.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading;
using Raven.Client.Http;
using Raven.Client.Util;

namespace Raven.Client.Documents.Changes
{
    internal class EvictItemsFromCacheBasedOnChanges : IObserver<DocumentChange>, IObserver<IndexChange>, IDisposable
    {
        private readonly string _databaseName;
        private readonly IDatabaseChanges _changes;
        private readonly IDisposable _documentsSubscription;
        private readonly IDisposable _indexesSubscription;
        private readonly RequestExecutor _requestExecutor;

        public EvictItemsFromCacheBasedOnChanges(DocumentStore store, string databaseName)
        {
            _databaseName = databaseName;
            _changes = store.Changes(databaseName);
            _requestExecutor = store.GetRequestExecutor(databaseName);
            var docSub = _changes.ForAllDocuments();
            _documentsSubscription = docSub.Subscribe(this);
            var indexSub = _changes.ForAllIndexes();
            _indexesSubscription = indexSub.Subscribe(this);
        }

        public void EnsureConnected()
        {
            AsyncHelpers.RunSync(_changes.EnsureConnectedNow);
        }

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

        public void OnCompleted()
        {
        }

        public void Dispose()
        {
            using (_changes)
            {
                _documentsSubscription.Dispose();
                _indexesSubscription.Dispose();
            }
        }
    }
}
