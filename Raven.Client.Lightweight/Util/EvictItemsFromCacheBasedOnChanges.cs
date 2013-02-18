// -----------------------------------------------------------------------
//  <copyright file="EvictItemsFromCacheBasedOnChanges.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
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

		public EvictItemsFromCacheBasedOnChanges(string databaseName, IDatabaseChanges changes, Action<string> evictCacheOldItems)
		{
			this.databaseName = databaseName;
			this.changes = changes;
			this.evictCacheOldItems = evictCacheOldItems;

			documentsSubscription = changes.ForAllDocuments().Subscribe(this);
			indexesSubscription = changes.ForAllIndexes().Subscribe(this);
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
			if (change.Type == IndexChangeTypes.MapCompleted || 
				change.Type == IndexChangeTypes.ReduceCompleted || 
				change.Type == IndexChangeTypes.IndexRemoved)
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
			}
		}
	}
}