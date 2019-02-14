// -----------------------------------------------------------------------
//  <copyright file="AsyncDocumentSubscriptions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Lambda2Js;
using Raven.Client.Documents.Commands;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Sparrow.Collections;
using Sparrow.Json;

namespace Raven.Client.Documents.Subscriptions
{
    public class DocumentSubscriptions : IDisposable
    {
        private readonly IDocumentStore _store;
        private readonly ConcurrentSet<IAsyncDisposable> _subscriptions = new ConcurrentSet<IAsyncDisposable>();

        public DocumentSubscriptions(IDocumentStore store)
        {
            _store = store;
        }

        /// <summary>
        /// Creates a data subscription in a database. The subscription will expose all documents that match the specified subscription options for a given type.
        /// </summary>
        /// <typeparam name="T">Type of the collection to be processed by the subscription</typeparam>
        /// <returns>Created subscription</returns>
        public string Create<T>(SubscriptionCreationOptions<T> options, string database = null)
        {
            return Create(EnsureCriteria(new SubscriptionCreationOptions
            {
                Name = options.Name,
                ChangeVector = options.ChangeVector
            }, options.Filter, options.Projection), database);

        }


        /// <summary>
        /// Creates a data subscription in a database. The subscription will expose all documents that match the specified subscription options for a given type.
        /// </summary>
        /// <typeparam name="T">Type of the collection to be processed by the subscription</typeparam>
        /// <returns>Created subscription</returns>
        public string Create<T>(Expression<Func<T, bool>> predicate = null,
            SubscriptionCreationOptions options = null,
            string database = null)
        {
            return Create(EnsureCriteria(options, predicate, null), database);
        }


        /// <summary>
        /// It creates a data subscription in a database. The subscription will expose all documents that match the specified subscription options.
        /// </summary>
        /// <returns>Created subscription name.</returns>
        public Task<string> CreateAsync<T>(
            SubscriptionCreationOptions<T> options, string database = null, CancellationToken token = default)
        {
            return CreateAsync(EnsureCriteria(new SubscriptionCreationOptions
            {
                Name = options.Name,
                ChangeVector = options.ChangeVector,
                MentorNode = options.MentorNode
            }, options.Filter, options.Projection), database, token);
        }

        /// <summary>
        /// It creates a data subscription in a database. The subscription will expose all documents that match the specified subscription options for a given type.
        /// </summary>
        /// <returns>Created subscription name.</returns>
        public Task<string> CreateAsync<T>(
            Expression<Func<T, bool>> predicate = null,
            SubscriptionCreationOptions options = null,
            string database = null, 
            CancellationToken token = default)
        {
            return CreateAsync(EnsureCriteria(options, predicate, null), database, token);
        }

        private SubscriptionCreationOptions EnsureCriteria<T>(
            SubscriptionCreationOptions criteria,
            Expression<Func<T, bool>> predicate,
            Expression<Func<T, object>> project)
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
                if (includeRevisions)
                    criteria.Query = "from " + collectionName + " (Revisions = true)";
                else
                    criteria.Query = "from " + collectionName;
                criteria.Query += " as doc";
            }            
            if (predicate != null)
            {
                var script = predicate.CompileToJavascript(
                    new JavascriptCompilationOptions(
                        JsCompilationFlags.BodyOnly,
                        JavascriptConversionExtensions.MathSupport.Instance,
                        new JavascriptConversionExtensions.DictionarySupport(),
                        JavascriptConversionExtensions.LinqMethodsSupport.Instance,
                        new JavascriptConversionExtensions.SubscriptionsWrappedConstantSupport(_store.Conventions),
                        new JavascriptConversionExtensions.ConstSupport(_store.Conventions),
                        new JavascriptConversionExtensions.ReplaceParameterWithNewName(predicate.Parameters[0], "this"),
                        JavascriptConversionExtensions.ToStringSupport.Instance,
                        new JavascriptConversionExtensions.DateTimeSupport(),
                        JavascriptConversionExtensions.InvokeSupport.Instance,
                        JavascriptConversionExtensions.NullCoalescingSupport.Instance,
                        JavascriptConversionExtensions.NestedConditionalSupport.Instance,
                        JavascriptConversionExtensions.StringSupport.Instance                        
                    ));

                criteria.Query = $"declare function predicate() {{ return {script} }}{Environment.NewLine}" +
                                 $"{criteria.Query}{Environment.NewLine}" +
                                 "where predicate.call(doc)";
            }
            if (project != null)
            {
                var script = project.CompileToJavascript(
                    new JavascriptCompilationOptions(
                        JsCompilationFlags.BodyOnly,
                        JavascriptConversionExtensions.MathSupport.Instance,
                        new JavascriptConversionExtensions.DictionarySupport(),
                        JavascriptConversionExtensions.LinqMethodsSupport.Instance,
                        new JavascriptConversionExtensions.ConstSupport(_store.Conventions),
                        JavascriptConversionExtensions.ToStringSupport.Instance,
                        new JavascriptConversionExtensions.DateTimeSupport(),
                        JavascriptConversionExtensions.InvokeSupport.Instance,
                        JavascriptConversionExtensions.NullCoalescingSupport.Instance,
                        JavascriptConversionExtensions.StringSupport.Instance,
                        JavascriptConversionExtensions.NestedConditionalSupport.Instance,                        
                        new JavascriptConversionExtensions.ReplaceParameterWithNewName(project.Parameters[0], "doc"),
                        JavascriptConversionExtensions.CounterSupport.Instance,
                        JavascriptConversionExtensions.CompareExchangeSupport.Instance
                    ));
                criteria.Query += Environment.NewLine + "select " + script;
            }
            return criteria;
        }

        /// <summary>
        /// Create a data subscription in a database. The subscription will expose all documents that match the specified subscription options for a given type.
        /// </summary>
        /// <param name="options"></param>
        /// <param name="database"></param>
        /// <returns>Subscription object</returns>
        public string Create(SubscriptionCreationOptions options, string database = null)
        {
            return AsyncHelpers.RunSync(() => CreateAsync(options, database));
        }


        /// <summary>
        /// It creates a data subscription in a database. The subscription will expose all documents that match the specified subscription options.
        /// </summary>
        /// <returns>Created subscription name.</returns>
        public async Task<string> CreateAsync(SubscriptionCreationOptions options, string database = null, CancellationToken token = default)
        {            
            if (options == null)
                throw new InvalidOperationException("Cannot create a subscription if options is null");

            if (options.Query == null)
                throw new InvalidOperationException("Cannot create a subscription if the script is null");

            var requestExecutor = _store.GetRequestExecutor(database ?? _store.Database);
            using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var command = new CreateSubscriptionCommand(_store.Conventions, options);
                await requestExecutor.ExecuteAsync(command, context, sessionInfo: null, token: token).ConfigureAwait(false);
                return command.Result.Name;
            }
        }

        /// <summary>
        /// It opens a subscription and starts pulling documents since a last processed document for that subscription.
        /// The connection options determine client and server cooperation rules like document batch sizes or a timeout in a matter of which a client
        /// needs to acknowledge that batch has been processed. The acknowledgment is sent after all documents are processed by subscription's handlers.  
        /// There can be only a single client that is connected to a subscription.
        /// </summary>
        /// <returns>Subscription object that allows to add/remove subscription handlers.</returns>
        public SubscriptionWorker<dynamic> GetSubscriptionWorker(SubscriptionWorkerOptions options, string database = null)
        {
            return GetSubscriptionWorker<dynamic>(options, database);
        }

        /// <summary>
        /// It opens a subscription and starts pulling documents since a last processed document for that subscription.
        /// Although this overload does not an <c>SubscriptionConnectionOptions</c> object as a parameter, it uses it's default values.
        /// The connection options determine client and server cooperation rules like document batch sizes or a timeout in a matter of which a client
        /// needs to acknowledge that batch has been processed. The acknowledgment is sent after all documents are processed by subscription's handlers.  
        /// There can be only a single client that is connected to a subscription.
        /// </summary>
        /// <returns>Subscription object that allows to add/remove subscription handlers.</returns>
        public SubscriptionWorker<dynamic> GetSubscriptionWorker(string subscriptionName, string database = null)
        {            
            return GetSubscriptionWorker<dynamic>(new SubscriptionWorkerOptions(subscriptionName), database);
        }

        /// <summary>
        /// It opens a subscription and starts pulling documents since a last processed document for that subscription.
        /// The connection options determine client and server cooperation rules like document batch sizes or a timeout in a matter of which a client
        /// needs to acknowledge that batch has been processed. The acknowledgment is sent after all documents are processed by subscription's handlers.  
        /// There can be only a single client that is connected to a subscription.
        /// </summary>
        /// <returns>Subscription object that allows to add/remove subscription handlers.</returns>
        public SubscriptionWorker<T> GetSubscriptionWorker<T>(SubscriptionWorkerOptions options, string database = null) where T : class
        {
            ((DocumentStoreBase)_store).AssertInitialized();
            if (options == null)
                throw new InvalidOperationException("Cannot open a subscription if options are null");

            var subscription = new SubscriptionWorker<T>(options, _store, database);
            subscription.OnDisposed += sender => _subscriptions.TryRemove(sender);
            _subscriptions.Add(subscription);

            return subscription;
        }

        /// <summary>
        /// It opens a subscription and starts pulling documents since a last processed document for that subscription.
        /// Although this overload does not an <c>SubscriptionConnectionOptions</c> object as a parameter, it uses it's default values.
        /// The connection options determine client and server cooperation rules like document batch sizes or a timeout in a matter of which a client
        /// needs to acknowledge that batch has been processed. The acknowledgment is sent after all documents are processed by subscription's handlers.  
        /// There can be only a single client that is connected to a subscription.
        /// </summary>
        /// <returns>Subscription object that allows to add/remove subscription handlers.</returns>
        public SubscriptionWorker<T> GetSubscriptionWorker<T>(string subscriptionName, string database = null) where T : class
        {
            return GetSubscriptionWorker<T>(new SubscriptionWorkerOptions(subscriptionName), database);
        }

        /// <summary>
        /// It downloads a list of all existing subscriptions in a database.
        /// </summary>
        /// <returns>Existing subscriptions' configurations.</returns>
        public async Task<List<SubscriptionState>> GetSubscriptionsAsync(int start, int take, string database = null, CancellationToken token = default)
        {
            var requestExecutor = _store.GetRequestExecutor(database ?? _store.Database);
            using (requestExecutor.ContextPool.AllocateOperationContext(out var jsonOperationContext))
            {
                var command = new GetSubscriptionsCommand(start, take);
                await requestExecutor.ExecuteAsync(command, jsonOperationContext, sessionInfo: null, token: token).ConfigureAwait(false);

                return command.Result.ToList();
            }
        }

        /// <summary>
        /// Delete a subscription.
        /// </summary>
        public async Task DeleteAsync(string name, string database = null, CancellationToken token = default)
        {
            (_store as DocumentStoreBase).AssertInitialized();
            var requestExecutor = _store.GetRequestExecutor(database ?? _store.Database);

            using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext jsonOperationContext))
            {
                var command = new DeleteSubscriptionCommand(name);
                await requestExecutor.ExecuteAsync(command, jsonOperationContext, sessionInfo: null, token: token).ConfigureAwait(false);
            }
        }


        /// <summary>
        /// Delete a subscription.
        /// </summary>
        public void Delete(string name, string database = null)
        {
            AsyncHelpers.RunSync(() => DeleteAsync(name, database));
        }


        /// <summary>
        /// Returns subscription definition and it's current state
        /// </summary>
        /// <param name="subscriptionName">Subscription name as received from the server</param>
        /// <param name="database">Database where the subscription resides</param>
        /// <returns></returns>
        public SubscriptionState GetSubscriptionState(string subscriptionName, string database = null)
        {
            return AsyncHelpers.RunSync(() => GetSubscriptionStateAsync(subscriptionName, database));
        }

        /// <summary>
        /// Returns subscription definition and it's current state
        /// </summary>
        /// <param name="subscriptionName">Subscription name as received from the server</param>
        /// <param name="database">Database where the subscription resides</param>
        /// <returns></returns>
        public async Task<SubscriptionState> GetSubscriptionStateAsync(string subscriptionName, string database = null, CancellationToken token = default)
        {

            if (string.IsNullOrEmpty(subscriptionName))
                throw new ArgumentNullException("SubscriptionName");

            var requestExecutor = _store.GetRequestExecutor(database);
            using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var command = new GetSubscriptionStateCommand(subscriptionName);
                await requestExecutor.ExecuteAsync(command, context, sessionInfo: null, token: token).ConfigureAwait(false);
                return command.Result;
            }
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

        /// <summary>
        /// It downloads a list of all existing subscriptions in a database.
        /// </summary>
        /// <returns>Existing subscriptions' configurations.</returns>
        public List<SubscriptionState> GetSubscriptions(int start, int take, string database = null)
        {
            return AsyncHelpers.RunSync(() => GetSubscriptionsAsync(start, take, database));
        }

        /// <summary>
        /// Force server to close current client subscription connection to the server
        /// </summary>
        /// <param name="id"></param>
        /// <param name="database"></param>
        public void DropConnection(string name, string database = null)
        {
            AsyncHelpers.RunSync(() => DropConnectionAsync(name, database));
        }

        /// <summary>
        /// Force server to close current client subscription connection to the server
        /// </summary>
        /// <param name="id"></param>
        /// <param name="database"></param>
        public async Task DropConnectionAsync(string name, string database = null, CancellationToken token = default)
        {
            var requestExecutor = _store.GetRequestExecutor(database ?? _store.Database);
            using (requestExecutor.ContextPool.AllocateOperationContext(out var jsonOperationContext))
            {
                var command = new DropSubscriptionConnectionCommand(name);
                await requestExecutor.ExecuteAsync(command, jsonOperationContext, sessionInfo: null, token: token).ConfigureAwait(false);
            }
        }
    }
}
