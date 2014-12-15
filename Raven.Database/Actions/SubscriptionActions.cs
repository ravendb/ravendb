// -----------------------------------------------------------------------
//  <copyright file="SubscriptionActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Json.Linq;

namespace Raven.Database.Actions
{
	public class SubscriptionActions : ActionsBase
	{
		private class SubscriptionWorkContext
		{
			public string ConnectionId;
			public SubscriptionBatchOptions BatchOptions;
		}

		private readonly ConcurrentDictionary<long, SubscriptionWorkContext> openSubscriptions = new ConcurrentDictionary<long, SubscriptionWorkContext>();

		public SubscriptionActions(DocumentDatabase database, ILog log)
			: base(database, null, null, log)
		{
		}

		public long CreateSubscription(SubscriptionCriteria criteria)
		{
			long id = -1;

			Database.TransactionalStorage.Batch(accessor =>
			{
				id = accessor.General.GetNextIdentityValue(Constants.RavenSubscriptionsPrefix);

				var doc = new SubscriptionDocument
				{
					SubscriptionId = id,
					Criteria = criteria,
					AckEtag = Etag.Empty
				};

				accessor.Lists.Set(Constants.RavenSubscriptionsPrefix, id.ToString("D19"),  RavenJObject.FromObject(doc), UuidType.Subscriptions);
			});

			return id;
		}

		public bool TryOpenSubscription(long id, SubscriptionBatchOptions options, out string connectionId)
		{
			connectionId = Base62Util.Base62Random();

			var subscriptionWorkContext = new SubscriptionWorkContext
			{
				ConnectionId = connectionId,
				BatchOptions = options
			};

			return openSubscriptions.TryAdd(id, subscriptionWorkContext);
		}

		public void ReleaseSubscription(long id)
		{
			SubscriptionWorkContext context;
			openSubscriptions.TryRemove(id, out context);
		}

		public bool IsClosed(long id)
		{
			return openSubscriptions.ContainsKey(id) == false;
		}

		public void AcknowledgeBatchProcessed(long id, Etag lastEtag)
		{
			TransactionalStorage.Batch(accessor =>
			{
				var doc = GetSubscriptionDocument(id);

				doc.AckEtag = lastEtag;

				accessor.Lists.Set(Constants.RavenSubscriptionsPrefix, id.ToString("D19"), RavenJObject.FromObject(doc), UuidType.Subscriptions);
			});
		}

		public void AssertOpenSubscriptionConnection(long id, string connection)
		{
			SubscriptionWorkContext subscriptionContext;
			if (openSubscriptions.TryGetValue(id, out subscriptionContext) == false)
				throw new InvalidOperationException("There is no subscription with id: " + id + " being opened");

			if (subscriptionContext.ConnectionId.Equals(connection, StringComparison.OrdinalIgnoreCase) == false)
			{
				// prevent from concurrent working against the same subscription
				throw new InvalidOperationException("Subscription is being opened for a different connection");
			}
		}

		//public bool WaitForAcknowledgement(long id, TimeSpan timeout)
		//{
		//	SubscriptionWorkContext subscriptionContext;
		//	if (openSubscriptions.TryGetValue(id, out subscriptionContext) == false)
		//		return false;

		//	return subscriptionContext.Acknowledgement.WaitOne(timeout);
		//}

		//public bool WaitForNewDocuments(long id, TimeSpan timeout)
		//{
		//	SubscriptionWorkContext subscriptionContext;
		//	if (openSubscriptions.TryGetValue(id, out subscriptionContext) == false)
		//		return false;

		//	return subscriptionContext.NewDocuments.WaitOne(timeout);
		//}

		public SubscriptionBatchOptions GetBatchOptions(long id)
		{
			SubscriptionWorkContext subscriptionContext;
			if (openSubscriptions.TryGetValue(id, out subscriptionContext) == false)
				throw new InvalidOperationException("There is no open subscription with id: " + id);

			return subscriptionContext.BatchOptions;
		}

		public SubscriptionDocument GetSubscriptionDocument(long id)
		{
			SubscriptionDocument doc = null;

			TransactionalStorage.Batch(accessor =>
			{
				var listItem = accessor.Lists.Read(Constants.RavenSubscriptionsPrefix, id.ToString("D19"));

				if(listItem == null)
					return;

				doc = listItem.Data.JsonDeserialization<SubscriptionDocument>();
			});

			return doc;
		}

		//public bool HasMoreDocumentsToSent(long id)
		//{
		//	SubscriptionWorkContext subscriptionContext;
			
		//	if(openSubscriptions.TryGetValue(id, out subscriptionContext) == false)
		//		throw new InvalidOperationException("No such subscription: " + id);

		//	lock (subscriptionContext)
		//	{
		//		Etag lastDocEtag = null;

		//		Database.TransactionalStorage.Batch(accessor => lastDocEtag = accessor.Staleness.GetMostRecentDocumentEtag());

		//		var lastAckEtag = GetSubscriptionDocument(id).AckEtag;

		//		subscriptionContext.NewDocuments.Reset();

		//		return EtagUtil.IsGreaterThan(lastDocEtag, lastAckEtag);
		//	}
		//}

	}
}