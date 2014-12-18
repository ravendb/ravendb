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
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Json.Linq;

namespace Raven.Database.Actions
{
	public class SubscriptionActions : ActionsBase
	{
		private readonly ConcurrentDictionary<long, SubscriptionConnectionOptions> openSubscriptions = 
			new ConcurrentDictionary<long, SubscriptionConnectionOptions>();

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

		public void OpenSubscription(long id, SubscriptionConnectionOptions options)
		{
			if (openSubscriptions.TryAdd(id, options))
			{
				UpdateClientActivityDate(id);
				return;
			}

			SubscriptionConnectionOptions existingOptions;

			if(openSubscriptions.TryGetValue(id, out existingOptions) == false)
				throw new SubscriptionDoesNotExistExeption("Didn't get existing open subscription while it's expected. Subscription id: " + id);

			if (existingOptions.ConnectionId.Equals(options.ConnectionId, StringComparison.OrdinalIgnoreCase))
			{
				// reopen subscription on already existing connection - might happen after network connection problems the client tries to reopen

				UpdateClientActivityDate(id);
				return; 
			}

			var doc = GetSubscriptionDocument(id);

			if (SystemTime.UtcNow - doc.TimeOfLastClientActivity > TimeSpan.FromTicks(existingOptions.ClientAliveNotificationInterval.Ticks * 3))
			{
				// last connected client didn't send at least two 'client-alive' notifications - let the requesting client to open it

				ReleaseSubscription(id);
				openSubscriptions.TryAdd(id, options);
				return;
			}

			throw new SubscriptionInUseException("Subscription is already in use. There can be only a single open subscription connection per subscription.");
		}

		public void ReleaseSubscription(long id)
		{
			SubscriptionConnectionOptions options;
			openSubscriptions.TryRemove(id, out options);
		}

		public void AcknowledgeBatchProcessed(long id, Etag lastEtag)
		{
			TransactionalStorage.Batch(accessor =>
			{
				var doc = GetSubscriptionDocument(id);
				var options = GetBatchOptions(id);

				var timeSinceBatchSent = SystemTime.UtcNow - doc.TimeOfSendingLastBatch;
				if(timeSinceBatchSent > options.AcknowledgmentTimeout)
					throw new TimeoutException("The subscription cannot be acknowledged because the timeout has been reached.");

				doc.AckEtag = lastEtag;
				doc.TimeOfLastClientActivity = SystemTime.UtcNow;

				SaveSubscriptionDocument(id, doc);
			});
		}

		public void AssertOpenSubscriptionConnection(long id, string connection)
		{
			SubscriptionConnectionOptions options;
			if (openSubscriptions.TryGetValue(id, out options) == false)
				throw new SubscriptionClosedException("There is no subscription with id: " + id + " being opened");

			if (options.ConnectionId.Equals(connection, StringComparison.OrdinalIgnoreCase) == false)
			{
				// prevent from concurrent work of multiple clients against the same subscription
				throw new SubscriptionInUseException("Subscription is being opened for a different connection.");
			}
		}

		public SubscriptionBatchOptions GetBatchOptions(long id)
		{
			SubscriptionConnectionOptions options;
			if (openSubscriptions.TryGetValue(id, out options) == false)
				throw new InvalidOperationException("There is no open subscription with id: " + id);

			return options.BatchOptions;
		}

		public SubscriptionDocument GetSubscriptionDocument(long id)
		{
			SubscriptionDocument doc = null;

			TransactionalStorage.Batch(accessor =>
			{
				var listItem = accessor.Lists.Read(Constants.RavenSubscriptionsPrefix, id.ToString("D19"));

				if(listItem == null)
					throw new SubscriptionDoesNotExistExeption("There is no subscription configuration for specified identifier (id: " + id + ")");

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