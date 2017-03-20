// -----------------------------------------------------------------------
//  <copyright file="AsyncDocumentSubscriptions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Sparrow.Collections;
using Sparrow.Json;

namespace Raven.Client.Documents.Subscriptions
{
    public class AsyncDocumentSubscriptions : IAsyncReliableSubscriptions
    {
        private readonly IDocumentStore _store;
        private readonly ConcurrentSet<IDisposableAsync> _subscriptions = new ConcurrentSet<IDisposableAsync>();

        public AsyncDocumentSubscriptions(IDocumentStore store)
        {
            _store = store;
        }

        public Task<long> CreateAsync<T>(SubscriptionCriteria<T> criteria, long startEtag = 0, string database = null)
        {
            if (criteria == null)
                throw new InvalidOperationException("Cannot create a subscription if criteria is null");

            var nonGenericCriteria = new SubscriptionCriteria(_store.Conventions.GetCollectionName(typeof(T)))
            {
                FilterJavaScript = criteria.FilterJavaScript
            };

            return CreateAsync(nonGenericCriteria, startEtag, database);
        }

        public async Task<long> CreateAsync(SubscriptionCriteria criteria, long startEtag = 0, string database = null)
        {
            if (criteria == null)
                throw new InvalidOperationException("Cannot create a subscription if criteria is null");

            JsonOperationContext jsonOperationContext;
            var requestExecuter = _store.GetRequestExecuter(database ?? _store.DefaultDatabase);
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

            var subscription = new Subscription<T>(options, _store, _store.Conventions, database);
            subscription.SubscriptionConnectionInterrupted  += (exception, willReconnect) =>
            {
                if (willReconnect == false)
                    _subscriptions.TryRemove(subscription);
            };
            _subscriptions.Add(subscription);

            return subscription;
        }

        public async Task<List<SubscriptionConfig>> GetSubscriptionsAsync(int start, int take, string database = null)
        {
            JsonOperationContext jsonOperationContext;
            var requestExecuter = _store.GetRequestExecuter(database ?? _store.DefaultDatabase);
            requestExecuter.ContextPool.AllocateOperationContext(out jsonOperationContext);

            var command = new GetSubscriptionsCommand(start, take);
            await requestExecuter.ExecuteAsync(command, jsonOperationContext);

            return command.Result.ToList();
        }

        public async Task DeleteAsync(long id, string database = null)
        {
            JsonOperationContext jsonOperationContext;
            var requestExecuter = _store.GetRequestExecuter(database ?? _store.DefaultDatabase);
            requestExecuter.ContextPool.AllocateOperationContext(out jsonOperationContext);

            var command = new DeleteSubscriptionCommand(id);
            await requestExecuter.ExecuteAsync(command, jsonOperationContext);
        }

        public void Dispose()
        {
            if (_subscriptions.Count == 0)
                return;
            var tasks = new List<Task>();

            foreach (var subscription in _subscriptions)
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