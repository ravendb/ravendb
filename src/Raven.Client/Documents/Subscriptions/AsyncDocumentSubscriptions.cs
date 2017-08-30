// -----------------------------------------------------------------------
//  <copyright file="AsyncDocumentSubscriptions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Lambda2Js;
using Raven.Client.Documents.Commands;
using Raven.Client.Extensions;
using Raven.Client.ServerWide.Operations;
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

        public string Create<T>(Expression<Func<T, bool>> predicate = null,
            SubscriptionCreationOptions options = null,
            string database = null)
        {
            return Create(EnsureCriteria(options, predicate), database);
        }

        public Task<string> CreateAsync<T>(
            Expression<Func<T, bool>> predicate = null,
            SubscriptionCreationOptions options = null, 
            string database = null)
        {
            return CreateAsync(EnsureCriteria(options, predicate), database);
        }

        private SubscriptionCreationOptions EnsureCriteria<T>(SubscriptionCreationOptions criteria, Expression<Func<T, bool>> predicate)
        {
            criteria = criteria ?? new SubscriptionCreationOptions();
            var collectionName = _store.Conventions.GetCollectionName(typeof(T));
            if (criteria.Query == null)
            {
                var tType = typeof(T);
                var includeRevisions = tType.IsConstructedGenericType && tType.GetGenericTypeDefinition() == typeof(Revision<>);
                if (includeRevisions)
                {
                    collectionName = _store.Conventions.GetCollectionName(tType.GenericTypeArguments[0]);
                }
                if(includeRevisions)
                    criteria.Query = "from " + collectionName +" (Revisions = true)";
                else
                    criteria.Query = "from " + collectionName;
            }
            if (predicate != null)
            {
                var script = predicate.CompileToJavascript(
                    new JavascriptCompilationOptions(
                        JsCompilationFlags.BodyOnly,
                        new JavascriptConversionExtensions.LinqMethodsSupport(),
                        new JavascriptConversionExtensions.DatesAndConstantsSupport { Parameter = predicate.Parameters[0] }
                    ));
                criteria.Query = "declare function predicate () {\r\n\t return " + 
                    script + "\r\n}\r\n" + criteria.Query + "\r\n" + 
                    "where predicate.call(this)";
            }
            return criteria;
        }

        public string Create(SubscriptionCreationOptions criteria, string database = null)
        {
            return AsyncHelpers.RunSync(() => CreateAsync(criteria, database));
        }


        public async Task<string> CreateAsync(SubscriptionCreationOptions options, string database = null)
        {
            if (options == null )
                throw new InvalidOperationException("Cannot create a subscription if options is null");

            if (options.Query == null)
                throw new InvalidOperationException("Cannot create a subscription if the script is null");

            var requestExecutor = _store.GetRequestExecutor(database ?? _store.Database);
            requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context);
            
            var command = new CreateSubscriptionCommand(options);
            await requestExecutor.ExecuteAsync(command, context).ConfigureAwait(false);

            return command.Result.Name;
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

        public SubscriptionState GetSubscriptionState(string subscriptionName, string database = null)
        {
            return AsyncHelpers.RunSync(() => GetSubscriptionStateAsync(subscriptionName, database));
        }

        public async Task<SubscriptionState> GetSubscriptionStateAsync(string subscriptionName, string database= null)
        {

            if (string.IsNullOrEmpty(subscriptionName))
                throw new ArgumentNullException("SubscriptionName");

            var requestExecutor = _store.GetRequestExecutor(database);
            requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context);

            var command = new GetSubscriptionStateCommand(subscriptionName);
            await requestExecutor.ExecuteAsync(command, context).ConfigureAwait(false);
            return command.Result;
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
