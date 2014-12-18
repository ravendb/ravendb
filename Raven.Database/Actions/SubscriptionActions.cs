// -----------------------------------------------------------------------
//  <copyright file="SubscriptionActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Raven.Abstractions;
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
			public SubscriptionConnectionOptions ConnectionOptions;
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

				SaveSubscriptionDocument(id, doc);
			});

			return id;
		}

		private void SaveSubscriptionDocument(long id, SubscriptionDocument doc)
		{
			Database.TransactionalStorage.Batch(accessor => 
				accessor.Lists.Set(Constants.RavenSubscriptionsPrefix, id.ToString("D19"), RavenJObject.FromObject(doc), UuidType.Subscriptions));
		}

		public bool TryOpenSubscription(long id, string existingConnectionId, SubscriptionConnectionOptions options, out string connectionId)
		{
			connectionId = existingConnectionId ?? Base62Util.Base62Random();

			var subscriptionWorkContext = new SubscriptionWorkContext
			{
				ConnectionId = connectionId,
				ConnectionOptions = options
			};

			if (openSubscriptions.TryAdd(id, subscriptionWorkContext))
			{
				UpdateClientActivityDate(id);
				return true;
			}
			
			SubscriptionWorkContext existingSubscriptionContext;

			if(openSubscriptions.TryGetValue(id, out existingSubscriptionContext) == false)
				throw new InvalidOperationException("Didn't get existing open subscription while it's expected. Subscription id: " + id);

			if (existingConnectionId != null && existingSubscriptionContext.ConnectionId.Equals(existingConnectionId, StringComparison.OrdinalIgnoreCase))
				return true; // reopen subscription on already existing connection

			var doc = GetSubscriptionDocument(id);

			if (SystemTime.UtcNow - doc.TimeOfLastClientActivity > TimeSpan.FromTicks(existingSubscriptionContext.ConnectionOptions.ClientAliveNotificationInterval.Ticks * 3))
			{
				// last connected client didn't send at least two 'client-alive' notifications - let the requesting client to open it

				ReleaseSubscription(id);
				return openSubscriptions.TryAdd(id, subscriptionWorkContext);
			}
			return false;
		}

		public void ReleaseSubscription(long id)
		{
			SubscriptionWorkContext context;
			openSubscriptions.TryRemove(id, out context);
		}

		public void AcknowledgeBatchProcessed(long id, Etag lastEtag)
		{
			TransactionalStorage.Batch(accessor =>
			{
				var doc = GetSubscriptionDocument(id);
				var options = GetBatchOptions(id);

				var timeSinceBatchSent = SystemTime.UtcNow - doc.TimeOfSendingLastBatch;
				if(timeSinceBatchSent > options.BatchOptions.AcknowledgmentTimeout)
					throw new TimeoutException("The subscription cannot be acknowledged because the timeout has been reached.");

				doc.AckEtag = lastEtag;
				doc.TimeOfLastClientActivity = SystemTime.UtcNow;

				SaveSubscriptionDocument(id, doc);
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

		public SubscriptionConnectionOptions GetBatchOptions(long id)
		{
			SubscriptionWorkContext subscriptionContext;
			if (openSubscriptions.TryGetValue(id, out subscriptionContext) == false)
				throw new InvalidOperationException("There is no open subscription with id: " + id);

			return subscriptionContext.ConnectionOptions;
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

		public void UpdateBatchSentTime(long id)
		{
			TransactionalStorage.Batch(accessor =>
			{
				var doc = GetSubscriptionDocument(id);

				doc.TimeOfSendingLastBatch = SystemTime.UtcNow;
				doc.TimeOfLastClientActivity = SystemTime.UtcNow;

				SaveSubscriptionDocument(id, doc);
			});
		}

		public void UpdateClientActivityDate(long id)
		{
			TransactionalStorage.Batch(accessor =>
			{
				var doc = GetSubscriptionDocument(id);
				
				doc.TimeOfLastClientActivity = SystemTime.UtcNow;

				SaveSubscriptionDocument(id, doc);
			});
		}

		public List<SubscriptionDocument> GetSubscriptions(int start, int take)
		{
			var subscriptions = new List<SubscriptionDocument>();

			TransactionalStorage.Batch(accessor =>
			{
				foreach (var listItem in accessor.Lists.Read(Constants.RavenSubscriptionsPrefix, start, take))
				{
					var doc = listItem.Data.JsonDeserialization<SubscriptionDocument>();
					subscriptions.Add(doc);
				}
			});

			return subscriptions;
		}
	}
}