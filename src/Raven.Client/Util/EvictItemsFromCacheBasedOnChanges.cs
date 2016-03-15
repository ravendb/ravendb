// -----------------------------------------------------------------------
//  <copyright file="EvictItemsFromCacheBasedOnChanges.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Changes;

namespace Raven.Client.Util
{
    public class EvictItemsFromCacheBasedOnChanges : IObserver<DocumentChangeNotification>, IObserver<IndexChangeNotification>, IDisposable
    {
        private readonly string databaseName;
        private readonly IDatabaseChanges changes;
        private readonly Action<string> evictCacheOldItems;
        private readonly IDisposable documentsSubscription;
        private readonly IDisposable indexesSubscription;
        private readonly Task connectionTask;

        public EvictItemsFromCacheBasedOnChanges(string databaseName, IDatabaseChanges changes, Action<string> evictCacheOldItems)
        {
            this.databaseName = databaseName;
            this.changes = changes;
            this.evictCacheOldItems = evictCacheOldItems;
            var docSub = changes.ForAllDocuments();
            documentsSubscription = docSub.Subscribe(this);
            var indexSub = changes.ForAllIndexes();
            indexesSubscription = indexSub.Subscribe(this);

            connectionTask = Task.Factory.ContinueWhenAll(new Task[] { docSub.Task, indexSub.Task }, tasks => { });
        }

        public Task ConnectionTask
        {
            get { return connectionTask; }
        }

        public void OnNext(DocumentChangeNotification change)
        {
            if (change.Type == DocumentChangeTypes.Put || change.Type == DocumentChangeTypes.Delete)
            {
                evictCacheOldItems(databaseName);
            }
        }

        public void OnNext(IndexChangeNotification change)
        {
            if (change.Type == IndexChangeTypes.BatchCompleted || change.Type == IndexChangeTypes.IndexRemoved)
            {
                evictCacheOldItems(databaseName);
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
            documentsSubscription.Dispose();
            indexesSubscription.Dispose();
            using (changes as IDisposable)
            {
                var remoteDatabaseChanges = changes as RemoteDatabaseChanges;
                if (remoteDatabaseChanges != null)
                {
                    remoteDatabaseChanges.DisposeAsync().Wait(TimeSpan.FromSeconds(3));
                }
            }
        }
    }
}
