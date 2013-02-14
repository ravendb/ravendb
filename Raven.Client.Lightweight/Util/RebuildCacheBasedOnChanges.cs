// -----------------------------------------------------------------------
//  <copyright file="RebuildCacheBasedOnChanges.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;
using Raven.Abstractions.Data;
using Raven.Client.Changes;
using Raven.Client.Document;

namespace Raven.Client.Util
{
	public class RebuildCacheBasedOnChanges : IObserver<DocumentChangeNotification>, IObserver<IndexChangeNotification>, IDisposable
	{
		private readonly IDatabaseChanges changes;
		private readonly Action<DateTimeOffset, ICredentials, DocumentConvention> rebuildCache;
		private readonly ICredentials credentials;
		private readonly DocumentConvention conventions;
		private readonly IDisposable documentsSubscription;
		private readonly IDisposable indexesSubscription;

		public RebuildCacheBasedOnChanges(IDatabaseChanges changes, Action<DateTimeOffset, ICredentials, DocumentConvention> rebuildCache, ICredentials credentials, DocumentConvention conventions)
		{
			this.changes = changes;
			this.rebuildCache = rebuildCache;
			this.credentials = credentials;
			this.conventions = conventions;
			LastNotificationTime = DateTimeOffset.Now;

			documentsSubscription = changes.ForAllDocuments().Subscribe(this);
			indexesSubscription = changes.ForAllIndexes().Subscribe(this);
		}

		public DateTimeOffset LastNotificationTime { get; private set; }

		public void OnNext(DocumentChangeNotification change)
		{
			if (change.Type == DocumentChangeTypes.Put || change.Type == DocumentChangeTypes.Delete)
			{
				Rebuild();
			}
		}

		public void OnNext(IndexChangeNotification change)
		{
			if (change.Type != IndexChangeTypes.IndexAdded) // if new index was added it isn't already in cache
			{
				Rebuild();
			}
		}

		private void Rebuild()
		{
			var currentNotificationTime = DateTimeOffset.Now;

			lock (this)
			{
				rebuildCache(LastNotificationTime, credentials, conventions);
				LastNotificationTime = currentNotificationTime;
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