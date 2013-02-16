// -----------------------------------------------------------------------
//  <copyright file="EvictItemsFromCacheBasedOnChanges.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Client.Changes;

namespace Raven.Client.Util
{
	public class EvictItemsFromCacheBasedOnChanges : IObserver<DocumentChangeNotification>, IObserver<IndexChangeNotification>, IDisposable
	{
		private readonly IDatabaseChanges changes;
		private readonly Action evictCacheOldItems;
		private readonly IDisposable documentsSubscription;
		private readonly IDisposable indexesSubscription;

		public EvictItemsFromCacheBasedOnChanges(IDatabaseChanges changes, Action evictCacheOldItems)
		{
			this.changes = changes;
			this.evictCacheOldItems = evictCacheOldItems;

			documentsSubscription = changes.ForAllDocuments().Subscribe(this);
			indexesSubscription = changes.ForAllIndexes().Subscribe(this);
		}

		public DateTimeOffset LastNotificationTime { get; private set; }

		public void OnNext(DocumentChangeNotification change)
		{
			if (change.Type == DocumentChangeTypes.Put || change.Type == DocumentChangeTypes.Delete)
			{
				evictCacheOldItems();
			}
		}

		public void OnNext(IndexChangeNotification change)
		{
			if (change.Type == IndexChangeTypes.MapCompleted || 
				change.Type == IndexChangeTypes.ReduceCompleted || 
				change.Type == IndexChangeTypes.IndexRemoved)
			{
				evictCacheOldItems();
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