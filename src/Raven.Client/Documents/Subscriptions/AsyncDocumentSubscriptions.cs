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
    public class DocumentSubscriptions : IReliableSubscriptions
    {
        private readonly IDocumentStore _store;
        private readonly ConcurrentSet<IAsyncDisposable> _subscriptions = new ConcurrentSet<IAsyncDisposable>();

        public DocumentSubscriptions(IDocumentStore store)
        {
            _store = store;
        }

        public long Create(SubscriptionCreationOptions criteria, string database = null)
        {
            return AsyncHelpers.RunSync(() => CreateAsync(criteria, database));
        }

        public long Create<T>(SubscriptionCreationOptions<T> criteria, string database = null)
        {
            return AsyncHelpers.RunSync(() => CreateAsync(criteria, database));
        }

        public Task<long> CreateAsync<T>(SubscriptionCreationOptions<T> subscriptionCreationOptions, string database = null)
        {
            if (subscriptionCreationOptions == null)
                throw new InvalidOperationException("Cannot create a subscription if criteria is null");

            var subscriptionCreationDto = new SubscriptionCreationOptions
            {
                Name = subscriptionCreationOptions.Name,
                Criteria =  subscriptionCreationOptions.CreateOptions(_store.GetRequestExecutor(database).Conventions),
                ChangeVector = subscriptionCreationOptions.ChangeVector
            };

            return CreateAsync(subscriptionCreationDto, database);
        }

        public async Task<long> CreateAsync(SubscriptionCreationOptions subscriptionCreationOptions, string database = null)
        {
            if (subscriptionCreationOptions == null )
                throw new InvalidOperationException("Cannot create a subscription if subscriptionCreationOptions is null");

            if (subscriptionCreationOptions.Criteria == null)
                throw new InvalidOperationException("Cannot create a subscription if criteria is null");

            if (string.IsNullOrWhiteSpace(subscriptionCreationOptions.Criteria.Collection))
                throw new InvalidOperationException("Cannot create a subscription if criteria's collection is not set");

            var requestExecutor = _store.GetRequestExecutor(database ?? _store.Database);
            requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context);
            
            var command = new CreateSubscriptionCommand(subscriptionCreationOptions, context);
            await requestExecutor.ExecuteAsync(command, context).ConfigureAwait(false);

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
            
            var subscription = new Subscription<T>(options, _store, database);
            subscription.OnDisposed  += sender => _subscriptions.TryRemove(sender);
            _subscriptions.Add(subscription);

            return subscription;
        }

        public async Task<List<SubscriptionState>> GetSubscriptionsAsync(int start, int take, string database = null)
        {
            JsonOperationContext jsonOperationContext;
            var requestExecutor = _store.GetRequestExecutor(database ?? _store.Database);
            requestExecutor.ContextPool.AllocateOperationContext(out jsonOperationContext);

            var command = new GetSubscriptionsCommand(start, take);
            await requestExecutor.ExecuteAsync(command, jsonOperationContext).ConfigureAwait(false);

            return command.Result.ToList();
        }

        public async Task DeleteAsync(long id, string database = null)
        {
            JsonOperationContext jsonOperationContext;
            var requestExecutor = _store.GetRequestExecutor(database ?? _store.Database);
            requestExecutor.ContextPool.AllocateOperationContext(out jsonOperationContext);

            var command = new DeleteSubscriptionCommand(id.ToString());
            await requestExecutor.ExecuteAsync(command, jsonOperationContext).ConfigureAwait(false);
        }

        public async Task DeleteAsync(string name, string database = null)
        {
            JsonOperationContext jsonOperationContext;
            var requestExecutor = _store.GetRequestExecutor(database ?? _store.Database);
            requestExecutor.ContextPool.AllocateOperationContext(out jsonOperationContext);

            var command = new DeleteSubscriptionCommand(name);
            await requestExecutor.ExecuteAsync(command, jsonOperationContext).ConfigureAwait(false);
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