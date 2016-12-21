// -----------------------------------------------------------------------
//  <copyright file="AsyncDocumentSubscriptions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Connection;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Exceptions.Subscriptions;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Abstractions.Util;

using Raven.NewClient.Client.Util;
using Newtonsoft.Json;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Commands;
using Sparrow.Collections;
using Sparrow.Json;

namespace Raven.NewClient.Client.Document
{
    public class AsyncDocumentSubscriptions : IAsyncReliableSubscriptions
    {
        private readonly IDocumentStore documentStore;
        private readonly ConcurrentSet<IDisposableAsync> subscriptions = new ConcurrentSet<IDisposableAsync>();

        public AsyncDocumentSubscriptions(IDocumentStore documentStore)
        {
            this.documentStore = documentStore;
        }

        public Task<long> CreateAsync<T>(SubscriptionCriteria<T> criteria, long startEtag = 0, string database = null)
        {
            if (criteria == null)
                throw new InvalidOperationException("Cannot create a subscription if criteria is null");

            var nonGenericCriteria = new SubscriptionCriteria
            {
                Collection = documentStore.Conventions.GetTypeTagName(typeof (T)),
                FilterJavaScript = criteria.FilterJavaScript
            };


            return CreateAsync(nonGenericCriteria, startEtag, database);
        }

        public async Task<long> CreateAsync(SubscriptionCriteria criteria, long startEtag=0, string database = null)
        {
            if (criteria == null)
                throw new InvalidOperationException("Cannot create a subscription if criteria is null");
            
            JsonOperationContext jsonOperationContext;
            var requestExecuter = documentStore.GetRequestExecuter(database ?? documentStore.DefaultDatabase);
            requestExecuter.ContextPool.AllocateOperationContext(out jsonOperationContext);

            var command = new CreateSubscriptionCommand()
            {
                Context = jsonOperationContext,
                Criteria = criteria,
                StartEtag = startEtag
            };
            await requestExecuter.ExecuteAsync(command, jsonOperationContext);

            return command.Result.Id;
        }

        public Subscription<dynamic> Open(SubscriptionConnectionOptions options, string database = null)
        {
            return Open<dynamic>(options, database);
        }

        public Subscription<T> Open<T>(SubscriptionConnectionOptions options, string database = null) where T : class
        {
            if (options == null)
                throw new InvalidOperationException("Cannot open a subscription if options are null");

            // to ensure that subscription is open try to call it with the same connection id
            var subscription = new Subscription<T>(options, documentStore, documentStore.Conventions, database); 

            subscriptions.Add(subscription);

            return subscription;
        }

        public async Task<List<SubscriptionConfig>> GetSubscriptionsAsync(int start, int take, string database = null)
        {
            List<SubscriptionConfig> configs = new List<SubscriptionConfig>();

            JsonOperationContext jsonOperationContext;
            var requestExecuter = documentStore.GetRequestExecuter(database ?? documentStore.DefaultDatabase);
            requestExecuter.ContextPool.AllocateOperationContext(out jsonOperationContext);

            var command = new GetSubscriptionsCommand();
            await requestExecuter.ExecuteAsync(command, jsonOperationContext);

            foreach (BlittableJsonReaderObject document in command.Result.Results)
            {
                configs.Add((SubscriptionConfig)documentStore.Conventions.DeserializeEntityFromBlittable(typeof(SubscriptionConfig), document));
            }

            return configs;
        }

        public async Task DeleteAsync(long id, string database = null)
        {
            JsonOperationContext jsonOperationContext;
            var requestExecuter = documentStore.GetRequestExecuter(database ?? documentStore.DefaultDatabase);
            requestExecuter.ContextPool.AllocateOperationContext(out jsonOperationContext);

            var command = new DeleteSubscriptionCommand(id);
            await requestExecuter.ExecuteAsync(command, jsonOperationContext);
        }

        public async Task ReleaseAsync(long id, string database = null)
        {
            JsonOperationContext jsonOperationContext;
            var requestExecuter = documentStore.GetRequestExecuter(database ?? documentStore.DefaultDatabase);
            requestExecuter.ContextPool.AllocateOperationContext(out jsonOperationContext);

            var command = new ReleaseSubscriptionCommand(id);
            await requestExecuter.ExecuteAsync(command, jsonOperationContext);
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
            if (subscriptions.Count == 0)
                return;
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