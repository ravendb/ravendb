// -----------------------------------------------------------------------
//  <copyright file="SubscriptionChannel.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Client.Extensions;
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
	public class DocumentSubscriptions : IReliableSubscriptions
	{
		private readonly IDocumentStore documentStore;
		private readonly ConcurrentSet<Subscription> subscriptions = new ConcurrentSet<Subscription>(); 

		public DocumentSubscriptions(IDocumentStore documentStore)
		{
			this.documentStore = documentStore;
		}

		public long Create(SubscriptionCriteria criteria, string database = null)
		{
			if (criteria == null)
				throw new InvalidOperationException("Cannot create a subscription if criteria is null");

			var commands = database == null
				? documentStore.AsyncDatabaseCommands
				: documentStore.AsyncDatabaseCommands.ForDatabase(database);

			using (var request = commands.CreateRequest("/subscriptions/create", "POST"))
			{
				var response = request.ExecuteRawResponseAsync(RavenJObject.FromObject(criteria)).ResultUnwrap();
				response.AssertNotFailingResponse().WaitUnwrap();

				long subscriptionId;

				using (var stream = response.GetResponseStreamWithHttpDecompression().ResultUnwrap())
				using (var reader = new StreamReader(stream))
				{
					subscriptionId = long.Parse(reader.ReadToEnd());
				}

				return subscriptionId;
			}
		}

		public Subscription Open(long id, SubscriptionBatchOptions options, string database = null)
		{
			if(options == null)
				throw new InvalidOperationException("Cannot open a subscription if options are null");

			if(options.MaxSize.HasValue && options.MaxSize.Value < 16 * 1024)
				throw new InvalidOperationException("Max size value of batch options cannot be less that 16 KB");

			var commands = database == null
				? documentStore.AsyncDatabaseCommands
				: documentStore.AsyncDatabaseCommands.ForDatabase(database);

			string connectionId;

			using (var request = commands.CreateRequest("/subscriptions/open?id=" + id, "POST"))
			{
				try
				{
					var response = request.ExecuteRawResponseAsync(RavenJObject.FromObject(options)).ResultUnwrap();
					response.AssertNotFailingResponse().WaitUnwrap();

					using (var stream = response.GetResponseStreamWithHttpDecompression().ResultUnwrap())
		            using (var reader = new StreamReader(stream))
		            {
			            connectionId = reader.ReadToEnd();
		            }
				}
				catch (Exception e)
				{
					if (request.ResponseStatusCode == HttpStatusCode.NotFound)
						throw new InvalidOperationException("Subscription with the specified id does not exist.", e);

					if(request.ResponseStatusCode == HttpStatusCode.Gone)
						throw new InvalidOperationException("Subscription is already in use. There can be only a single open subscription connection per subscription.");

					throw;
				}
			}

			var subscription = new Subscription(id, connectionId, commands, documentStore.Changes(database));

			subscriptions.Add(subscription);

			return subscription;
		}

		public List<SubscriptionDocument> GetSubscriptions(int start, int take, string database = null)
		{
			var commands = database == null
				? documentStore.AsyncDatabaseCommands
				: documentStore.AsyncDatabaseCommands.ForDatabase(database);

			List<SubscriptionDocument> documents;

			using (var request = commands.CreateRequest("/subscriptions", "GET"))
			{
				var response = request.ReadResponseJson();

				documents = documentStore.Conventions.CreateSerializer().Deserialize<SubscriptionDocument[]>(new RavenJTokenReader(response)).ToList();
			}

			return documents;
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