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
		private class SubscriptionWorkContext : IDisposable
		{
			public readonly AutoResetEvent NewDocuments = new AutoResetEvent(false);
			public readonly AutoResetEvent Acknowledgement = new AutoResetEvent(false);
			public IDisposable Connection;

			public void Dispose()
			{
				NewDocuments.Set();
				NewDocuments.Dispose();

				Acknowledgement.Set();
				Acknowledgement.Dispose();

				//context.Connection.Dispose(); //TODO arek
			}
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
						lock (openSubscription.Value)
						{
							openSubscription.Value.NewDocuments.Set();
						}
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

		public Action<IDisposable> OpenSubscription(string name)
		{
			if (GetSubscriptionDocument(name) == null)
				throw new InvalidOperationException("Subscription " + name + "does not exit");

			var subscriptionWorkContext = new SubscriptionWorkContext();
			if(openSubscriptions.TryAdd(name, subscriptionWorkContext) == false)
				throw new InvalidOperationException("Subscription is already in use. There can be only a single open subscription request per subscription.");	

			return connection =>
			{
				subscriptionWorkContext.Connection = connection;
			};
		}

		public void ReleaseSubscription(string name)
		{
			SubscriptionWorkContext context;
			if (openSubscriptions.TryRemove(name, out context))
			{
				lock (context)
				{
					context.Dispose();
				}
			}
		}

		public bool IsClosed(string name)
		{
			return openSubscriptions.ContainsKey(name) == false;
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

			lock (subscriptionContext)
			{
				subscriptionContext.Acknowledgement.Set();
			}
		}

		public bool WaitForAcknowledgement(string name, TimeSpan timeout)
		{
			SubscriptionWorkContext subscriptionContext;
			if (openSubscriptions.TryGetValue(name, out subscriptionContext) == false)
				return false;

			return subscriptionContext.Acknowledgement.WaitOne(timeout);
		}

		public bool WaitForNewDocuments(string name, TimeSpan timeout)
		{
			SubscriptionWorkContext subscriptionContext;
			if (openSubscriptions.TryGetValue(name, out subscriptionContext) == false)
				return false;

			return subscriptionContext.NewDocuments.WaitOne(timeout);
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

		public bool HasMoreDocumentsToSent(string name)
		{
			SubscriptionWorkContext subscriptionContext;
			
			if(openSubscriptions.TryGetValue(name, out subscriptionContext) == false)
				throw new InvalidOperationException("No such subscription: " + name);

			lock (subscriptionContext)
			{
				Etag lastDocEtag = null;

				Database.TransactionalStorage.Batch(accessor => lastDocEtag = accessor.Staleness.GetMostRecentDocumentEtag());

				var lastAckEtag = GetSubscriptionDocument(name).AckEtag;

				subscriptionContext.NewDocuments.Reset();

				return EtagUtil.IsGreaterThan(lastDocEtag, lastAckEtag);
			}
		}
	}
}