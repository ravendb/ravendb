// -----------------------------------------------------------------------
//  <copyright file="SubscriptionChannel.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;

namespace Raven.Client.Document
{
	public class SubscriptionChannel : IReliableSubscriptions
	{
		private readonly IDocumentStore documentStore;

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
				? documentStore.AsyncDatabaseCommands.ForSystemDatabase()
				: documentStore.AsyncDatabaseCommands.ForDatabase(database);

			using (var request = commands.CreateRequest("/subscriptions/create?name=" + name, "POST"))
			{
				request.ExecuteRequest();

				var subscription = new Subscription(name, options, database, documentStore);

				return subscription;
			}
		}
	}
}