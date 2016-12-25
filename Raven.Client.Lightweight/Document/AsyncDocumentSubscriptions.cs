// -----------------------------------------------------------------------
//  <copyright file="AsyncDocumentSubscriptions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Abstractions.Util;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection.Async;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Sparrow.Collections;

namespace Raven.Client.Document
{
    public class AsyncDocumentSubscriptions : IAsyncReliableSubscriptions
    {
        private readonly IDocumentStore documentStore;
        private readonly ConcurrentSet<IDisposableAsync> subscriptions = new ConcurrentSet<IDisposableAsync>();

        public AsyncDocumentSubscriptions(IDocumentStore documentStore)
        {
            this.documentStore = documentStore;
        }

        public Task<long> CreateAsync<T>(SubscriptionCriteria<T> criteria, string database = null)
        {
            if (criteria == null)
                throw new InvalidOperationException("Cannot create a subscription if criteria is null");

            var nonGenericCriteria = new SubscriptionCriteria();

            nonGenericCriteria.BelongsToAnyCollection = new []{ documentStore.Conventions.GetTypeTagName(typeof (T)) };
            nonGenericCriteria.KeyStartsWith = criteria.KeyStartsWith;
            nonGenericCriteria.PropertiesMatch = criteria.GetPropertiesMatchStrings();
            nonGenericCriteria.PropertiesNotMatch = criteria.GetPropertiesNotMatchStrings();
            nonGenericCriteria.StartEtag = criteria.StartEtag;

            return CreateAsync(nonGenericCriteria, database);
        }

        public async Task<long> CreateAsync(SubscriptionCriteria criteria, string database = null)
        {
            if (criteria == null)
                throw new InvalidOperationException("Cannot create a subscription if criteria is null");

            var commands = database == null
                ? documentStore.AsyncDatabaseCommands
                : documentStore.AsyncDatabaseCommands.ForDatabase(database);

            using (var request = commands.CreateRequest("/subscriptions/create", HttpMethods.Post))
            {
                await request.WriteAsync(RavenJObject.FromObject(criteria)).ConfigureAwait(false);

                return request.ReadResponseJson().Value<long>("Id");
            }
        }

        public Task<Subscription<RavenJObject>> OpenAsync(long id, SubscriptionConnectionOptions options, string database = null)
        {
            return OpenAsync<RavenJObject>(id, options, database);
        }

        public async Task<Subscription<T>> OpenAsync<T>(long id, SubscriptionConnectionOptions options, string database = null) where T : class
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

            var open = true;

            try
            {
                await SendOpenSubscriptionRequest(commands, id, options).ConfigureAwait(false);
            }
            catch (SubscriptionException subscriptionException)
            {
                if (options.Strategy != SubscriptionOpeningStrategy.WaitForFree || (subscriptionException is SubscriptionInUseException) == false)
                    throw;

                open = false;
            }

            var subscription = new Subscription<T>(id, database ?? MultiDatabase.GetDatabaseName(documentStore.Url), options, commands, documentStore.Changes(database), 
                documentStore.Conventions, open, () => SendOpenSubscriptionRequest(commands, id, options)); // to ensure that subscription is open try to call it with the same connection id

            subscriptions.Add(subscription);
            subscription.OnDispose += () => subscriptions.TryRemove(subscription);

            return subscription;
        }

        private static async Task SendOpenSubscriptionRequest(IAsyncDatabaseCommands commands, long id, SubscriptionConnectionOptions options)
        {
            using (var request = commands.CreateRequest(string.Format("/subscriptions/open?id={0}&connection={1}", id, options.ConnectionId), HttpMethods.Post))
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

        public async Task<List<SubscriptionConfig>> GetSubscriptionsAsync(int start, int take, string database = null)
        {
            var commands = database == null
                ? documentStore.AsyncDatabaseCommands
                : documentStore.AsyncDatabaseCommands.ForDatabase(database);

            List<SubscriptionConfig> configs;

            using (var request = commands.CreateRequest("/subscriptions", HttpMethods.Get))
            {
                var response = await request.ReadResponseJsonAsync().ConfigureAwait(false);

                configs = documentStore.Conventions.CreateSerializer().Deserialize<SubscriptionConfig[]>(new RavenJTokenReader(response)).ToList();
            }

            return configs;
        }

        public async Task DeleteAsync(long id, string database = null)
        {
            var commands = database == null
                ? documentStore.AsyncDatabaseCommands
                : documentStore.AsyncDatabaseCommands.ForDatabase(database);

            using (var request = commands.CreateRequest("/subscriptions?id=" + id, HttpMethods.Delete))
            {
                await request.ExecuteRequestAsync().ConfigureAwait(false);
            }
        }

        public async Task ReleaseAsync(long id, string database = null)
        {
            var commands = database == null
                ? documentStore.AsyncDatabaseCommands
                : documentStore.AsyncDatabaseCommands.ForDatabase(database);

            using (var request = commands.CreateRequest(string.Format("/subscriptions/close?id={0}&connection=&force=true", id), HttpMethods.Post))
            {
                await request.ExecuteRequestAsync().ConfigureAwait(false);
            }
        }

        public static bool TryGetSubscriptionException(ErrorResponseException ere, out SubscriptionException subscriptionException)
        {
            var text = ere.ResponseString;

            if (ere.StatusCode == SubscriptionDoesNotExistException.RelevantHttpStatusCode)
            {
                var errorResult = JsonConvert.DeserializeAnonymousType(text, new
                {
                    url = (string)null,
                    error = (string)null
                });

                subscriptionException = new SubscriptionDoesNotExistException(errorResult.error);
                return true;
            }

            if (ere.StatusCode == SubscriptionInUseException.RelavantHttpStatusCode)
            {
                var errorResult = JsonConvert.DeserializeAnonymousType(text, new
                {
                    url = (string)null,
                    error = (string)null
                });

                subscriptionException = new SubscriptionInUseException(errorResult.error);
                return true;
            }

            if (ere.StatusCode == SubscriptionClosedException.RelevantHttpStatusCode)
            {
                var errorResult = JsonConvert.DeserializeAnonymousType(text, new
                {
                    url = (string)null,
                    error = (string)null
                });

                subscriptionException = new SubscriptionClosedException(errorResult.error);
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

            try
            {
                Task.WaitAll(tasks.ToArray(), TimeSpan.FromSeconds(3));
            }
            catch (AggregateException ae)
            {
                throw new InvalidOperationException("Failed to dispose active data subscriptions", ae.ExtractSingleInnerException());
            }
        }
    }
}
