// -----------------------------------------------------------------------
//  <copyright file="EvictItemsFromCacheBasedOnChanges.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Changes
{
    internal class EvictItemsFromCacheBasedOnChanges : IObserver<DocumentChange>, IObserver<IndexChange>, IDisposable
    {
        private readonly string _databaseName;
        private readonly IDatabaseChanges _changes;
        private readonly Action<string> _evictCacheOldItems;
        private readonly IDisposable _documentsSubscription;
        private readonly IDisposable _indexesSubscription;
        private readonly Task _connectionTask;

        public EvictItemsFromCacheBasedOnChanges(string databaseName, IDatabaseChanges changes, Action<string> evictCacheOldItems)
        {
            _databaseName = databaseName;
            _changes = changes;
            _evictCacheOldItems = evictCacheOldItems;
            var docSub = changes.ForAllDocuments();
            _documentsSubscription = docSub.Subscribe(this);
            var indexSub = changes.ForAllIndexes();
            _indexesSubscription = indexSub.Subscribe(this);

            //connectionTask = Task.Factory.ContinueWhenAll(new Task[] { docSub.Task, indexSub.Task }, tasks => { }, TaskContinuationOptions.RunContinuationsAsynchronously);
        }

        public Task ConnectionTask => _connectionTask;

        public void OnNext(DocumentChange change)
        {
            if (change.Type == DocumentChangeTypes.Put || change.Type == DocumentChangeTypes.Delete)
            {
                _evictCacheOldItems(_databaseName);
            }
        }

        public void OnNext(IndexChange change)
        {
            if (change.Type == IndexChangeTypes.BatchCompleted || change.Type == IndexChangeTypes.IndexRemoved)
            {
                _evictCacheOldItems(_databaseName);
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
            _documentsSubscription.Dispose();
            _indexesSubscription.Dispose();
            using (_changes as IDisposable)
            {
                //var remoteDatabaseChanges = changes as RemoteDatabaseChanges;
                //if (remoteDatabaseChanges != null)
                //{
                //    throw new NotImplementedException();

                //    remoteDatabaseChanges.DisposeAsync().Wait(TimeSpan.FromSeconds(3));
                //}
            }
        }
    }
}
