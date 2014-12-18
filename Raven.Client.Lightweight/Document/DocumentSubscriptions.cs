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
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Client.Connection.Async;
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
				request.WriteAsync(RavenJObject.FromObject(criteria)).WaitUnwrap();

				return request.ReadResponseJson().Value<long>("Id");
			}
		}

		public Subscription Open(long id, SubscriptionConnectionOptions options, string database = null)
		{
			if(options == null)
				throw new InvalidOperationException("Cannot open a subscription if options are null");

			if(options.BatchOptions == null)
				throw new InvalidOperationException("Cannot open a subscription if batch options are null");

			if(options.BatchOptions.MaxSize.HasValue && options.BatchOptions.MaxSize.Value < 16 * 1024)
				throw new InvalidOperationException("Max size value of batch options cannot be lower than that 16 KB");

			var commands = database == null
				? documentStore.AsyncDatabaseCommands
				: documentStore.AsyncDatabaseCommands.ForDatabase(database);

			SendOpenSubscriptionRequest(commands, id, options).WaitUnwrap();

			var subscription = new Subscription(id, options, commands, documentStore.Changes(database), () => 
				SendOpenSubscriptionRequest(commands, id, options)); // to ensure that subscription is open try to call it with the same connection id

			subscriptions.Add(subscription);

			return subscription;
		}

		private static async Task SendOpenSubscriptionRequest(IAsyncDatabaseCommands commands, long id, SubscriptionConnectionOptions options)
		{
			using (var request = commands.CreateRequest(string.Format("/subscriptions/open?id={0}&connection={1}", id, options.ConnectionId), "POST"))
			{
				try
				{
					await request.WriteAsync(RavenJObject.FromObject(options)).ConfigureAwait(false);
					await request.ExecuteRequestAsync().ConfigureAwait(false);
				}
				catch (ErrorResponseException e)
				{
					SubscriptionException subscriptionException;
					if (TryGetSubscriptionException(e, out subscriptionException))
						throw subscriptionException;

					throw;
				}
			}
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

		public static bool TryGetSubscriptionException(ErrorResponseException ere, out SubscriptionException subscriptionException)
		{
			if (ere.StatusCode == SubscriptionDoesNotExistExeption.RelevantHttpStatusCode)
			{
				subscriptionException = new SubscriptionDoesNotExistExeption(ere.ResponseString);
				return true;
			}

			if (ere.StatusCode == SubscriptionInUseException.RelavantHttpStatusCode)
			{
				subscriptionException = new SubscriptionInUseException(ere.Message);
				return true;
			}

			if (ere.StatusCode == SubscriptionClosedException.RelevantHttpStatusCode)
			{
				subscriptionException = new SubscriptionClosedException(ere.Message);
				return true;
			}

			subscriptionException = null;
			return false;
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