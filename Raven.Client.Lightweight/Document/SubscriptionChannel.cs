// -----------------------------------------------------------------------
//  <copyright file="SubscriptionChannel.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Database.Util;

namespace Raven.Client.Document
{
	public class SubscriptionChannel : IReliableSubscriptions
	{
		private readonly IDocumentStore documentStore;
		private readonly ConcurrentSet<Subscription> subscriptions = new ConcurrentSet<Subscription>(); 

		public SubscriptionChannel(IDocumentStore documentStore)
		{
			this.documentStore = documentStore;
		}

		public Subscription Create(string name, SubscriptionCriteria criteria, SubscriptionBatchOptions options, string database = null)
		{
			if (criteria == null)
				throw new InvalidOperationException("Cannot create a subscription if criteria is null");

			if (options == null)
				throw new InvalidOperationException("Cannot create a subscription if options are null");

			var commands = database == null
				? documentStore.AsyncDatabaseCommands
				: documentStore.AsyncDatabaseCommands.ForDatabase(database);

			using (var request = commands.CreateRequest("/subscriptions/create?name=" + name, "POST"))
			{
				request.ExecuteRequest();

				var subscription = new Subscription(name, options, commands);

				subscriptions.Add(subscription);

				return subscription;
			}
		}

		public void Dispose()
		{
			var tasks = new List<Task>();

			foreach (var subscription in subscriptions)
			{
				tasks.Add(subscription.DisposeAsync());
			}

			Task.WaitAll(tasks.ToArray(), TimeSpan.FromSeconds(3));
		}
	}
}