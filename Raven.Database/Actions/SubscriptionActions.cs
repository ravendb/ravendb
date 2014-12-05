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
using Raven.Json.Linq;

namespace Raven.Database.Actions
{
	public class SubscriptionActions : ActionsBase
	{
		private class SubscriptionWorkContext
		{
			public readonly AutoResetEvent NewDocuments = new AutoResetEvent(false);
			public readonly AutoResetEvent Acknowledgement = new AutoResetEvent(false);
		}

		private readonly ConcurrentDictionary<string, SubscriptionWorkContext> openSubscriptions = new ConcurrentDictionary<string, SubscriptionWorkContext>();

		public SubscriptionActions(DocumentDatabase database, ILog log)
			: base(database, null, null, log)
		{
			//TODO arek - should also update on BulkInsertEnd operation

			Database.Notifications.OnDocumentChange += (db, notification, metadata) =>
			{
				if (notification.Id == null)
					return;

				if (notification.Type == DocumentChangeTypes.Put && notification.Id.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase) == false)
				{
					foreach (var openSubscription in openSubscriptions)
					{
						openSubscription.Value.NewDocuments.Set();
					}
				}
			};
		}

		public void CreateSubscription(string name, SubscriptionCriteria criteria)
		{
			if (string.IsNullOrEmpty(name))
				throw new InvalidOperationException("Subscription must have a name");


			Database.TransactionalStorage.Batch(accessor =>
			{
				var subscriptionDocument = GetSubscriptionDocument(name);

				if (subscriptionDocument != null)
				{
					throw new InvalidOperationException("Subscription already exists."); // TODO arek
				}

				var doc = new SubscriptionDocument
				{
					Name = name,
					Criteria = criteria,
					AckEtag = Etag.Empty
				};

				accessor.Lists.Set(Constants.RavenSubscriptionsPrefix, name,  RavenJObject.FromObject(doc), UuidType.Subscriptions);
			});
		}

		public void OpenSubscription(string name)
		{
			if (GetSubscriptionDocument(name) == null)
				throw new InvalidOperationException("Subscription " + name + "does not exit");

			if(openSubscriptions.TryAdd(name, new SubscriptionWorkContext()) == false)
				throw new InvalidOperationException("Subscription is already in use. There can be only a single open subscription request per subscription.");	
		}

		public void ReleaseSubscription(string name)
		{
			SubscriptionWorkContext _;
			openSubscriptions.TryRemove(name, out _);
		}

		public void AcknowledgeBatchProcessed(string name, Etag lastEtag)
		{
			SubscriptionWorkContext subscriptionContext;
			if(openSubscriptions.TryGetValue(name, out subscriptionContext) == false)
				throw new InvalidOperationException("There is no subscription with name: " + name + " being opened");

			TransactionalStorage.Batch(accessor =>
			{
				var doc = GetSubscriptionDocument(name);

				doc.AckEtag = lastEtag;

				accessor.Lists.Set(Constants.RavenSubscriptionsPrefix, name, RavenJObject.FromObject(doc), UuidType.Subscriptions);
			});

			subscriptionContext.Acknowledgement.Set();
		}

		public bool WaitForAcknowledgement(string name, TimeSpan timeout)
		{
			SubscriptionWorkContext subscriptionContext;
			if (openSubscriptions.TryGetValue(name, out subscriptionContext) == false)
				return false;

			return subscriptionContext.Acknowledgement.WaitOne(timeout);
		}

		public bool WaitForNewDocuments(string name)
		{
			SubscriptionWorkContext subscriptionContext;
			if (openSubscriptions.TryGetValue(name, out subscriptionContext) == false)
				return false;

			return subscriptionContext.NewDocuments.WaitOne(-1);
		}

		public SubscriptionDocument GetSubscriptionDocument(string name)
		{
			SubscriptionDocument doc = null;

			TransactionalStorage.Batch(accessor =>
			{
				var listItem = accessor.Lists.Read(Constants.RavenSubscriptionsPrefix, name);

				if(listItem == null)
					return;

				doc = listItem.Data.JsonDeserialization<SubscriptionDocument>();
			});

			return doc;
		}
	}
}