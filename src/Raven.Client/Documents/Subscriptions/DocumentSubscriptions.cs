// -----------------------------------------------------------------------
//  <copyright file="AsyncDocumentSubscriptions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Lambda2Js;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Loaders;
using Raven.Client.Documents.Session.Tokens;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Sparrow.Collections;
using Sparrow.Json;

namespace Raven.Client.Documents.Subscriptions
{
    public class DocumentSubscriptions : IDisposable
    {
        private readonly DocumentStore _store;
        private readonly ConcurrentSet<IAsyncDisposable> _subscriptions = new ConcurrentSet<IAsyncDisposable>();

        public DocumentSubscriptions(IDocumentStore store)
        {
            _store = (DocumentStore)store;
        }

        /// <summary>
        /// Creates a data subscription in a database. The subscription will expose all documents that match the specified subscription options for a given type.
        /// </summary>
        /// <typeparam name="T">Type of the collection to be processed by the subscription</typeparam>
        /// <returns>Created subscription</returns>
        public string Create<T>(SubscriptionCreationOptions<T> options, string database = null)
        {
            return Create(CreateSubscriptionOptionsFromGeneric(_store.Conventions, new SubscriptionCreationOptions
            {
                Name = options.Name,
                ChangeVector = options.ChangeVector,
                MentorNode = options.MentorNode
            }, options.Filter, options.Projection, options.Includes), database);
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
            return Create(CreateSubscriptionOptionsFromGeneric(_store.Conventions, options, predicate, null, includes: null), database);
        }

        /// <summary>
        /// It creates a data subscription in a database. The subscription will expose all documents that match the specified subscription options.
        /// </summary>
        /// <returns>Created subscription name.</returns>
        public Task<string> CreateAsync<T>(
            SubscriptionCreationOptions<T> options, string database = null, CancellationToken token = default)
        {
            return CreateAsync(CreateSubscriptionOptionsFromGeneric(_store.Conventions, new SubscriptionCreationOptions
            {
                Name = options.Name,
                ChangeVector = options.ChangeVector,
                MentorNode = options.MentorNode
            }, options.Filter, options.Projection, options.Includes), database, token);
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
            return CreateAsync(CreateSubscriptionOptionsFromGeneric(_store.Conventions, options, predicate, null, includes: null), database, token);
        }

        internal static SubscriptionCreationOptions CreateSubscriptionOptionsFromGeneric<T>(
            DocumentConventions conventions,
            SubscriptionCreationOptions criteria,
            Expression<Func<T, bool>> predicate,
            Expression<Func<T, object>> project,
            Action<ISubscriptionIncludeBuilder<T>> includes)
        {
            criteria ??= new SubscriptionCreationOptions();
            var collectionName = conventions.GetCollectionName(typeof(T));
            StringBuilder queryBuilder;
            if (criteria.Query != null)
                queryBuilder = new StringBuilder(criteria.Query);
            else
            {
                queryBuilder = new StringBuilder();
                var tType = typeof(T);
                var includeRevisions = tType.IsConstructedGenericType && tType.GetGenericTypeDefinition() == typeof(Revision<>);
                if (includeRevisions)
                {
                    collectionName = conventions.GetCollectionName(tType.GenericTypeArguments[0]);
                }

                queryBuilder.Append("from '");
                StringExtensions.EscapeString(queryBuilder, collectionName);
                queryBuilder.Append('\'');
                if(includeRevisions)
                    queryBuilder.Append(" (Revisions = true)");

                criteria.Query = queryBuilder.Append(" as doc").ToString();
            }

            if (predicate != null)
            {
                var script = predicate.CompileToJavascript(
                    new JavascriptCompilationOptions(
                        JsCompilationFlags.BodyOnly,
                        JavascriptConversionExtensions.MathSupport.Instance,
                        new JavascriptConversionExtensions.DictionarySupport(),
                        JavascriptConversionExtensions.LinqMethodsSupport.Instance,
                        new JavascriptConversionExtensions.SubscriptionsWrappedConstantSupport(conventions),
                        new JavascriptConversionExtensions.ConstSupport(conventions),
                        new JavascriptConversionExtensions.ReplaceParameterWithNewName(predicate.Parameters[0], "this"),
                        JavascriptConversionExtensions.ToStringSupport.Instance,
                        JavascriptConversionExtensions.DateTimeSupport.Instance,
                        JavascriptConversionExtensions.InvokeSupport.Instance,
                        JavascriptConversionExtensions.NullCoalescingSupport.Instance,
                        JavascriptConversionExtensions.NestedConditionalSupport.Instance,
                        JavascriptConversionExtensions.StringSupport.Instance,
                        new JavascriptConversionExtensions.IdentityPropertySupport(conventions)
                    ));

                queryBuilder
                    .Insert(0, $"declare function predicate() {{ return {script} }}{Environment.NewLine}")
                    .AppendLine()
                    .Append("where predicate.call(doc)");
            }

            if (project != null)
            {
                var script = project.CompileToJavascript(
                    new JavascriptCompilationOptions(
                        JsCompilationFlags.BodyOnly,
                        JavascriptConversionExtensions.MathSupport.Instance,
                        new JavascriptConversionExtensions.DictionarySupport(),
                        JavascriptConversionExtensions.LinqMethodsSupport.Instance,
                        new JavascriptConversionExtensions.ConstSupport(conventions),
                        JavascriptConversionExtensions.ToStringSupport.Instance,
                        JavascriptConversionExtensions.DateTimeSupport.Instance,
                        JavascriptConversionExtensions.InvokeSupport.Instance,
                        JavascriptConversionExtensions.NullCoalescingSupport.Instance,
                        JavascriptConversionExtensions.StringSupport.Instance,
                        JavascriptConversionExtensions.NestedConditionalSupport.Instance,
                        new JavascriptConversionExtensions.ReplaceParameterWithNewName(project.Parameters[0], "doc"),
                        JavascriptConversionExtensions.CounterSupport.Instance,
                        JavascriptConversionExtensions.CompareExchangeSupport.Instance
                    ));

                queryBuilder
                    .AppendLine()
                    .Append("select ")
                    .Append(script);
            }

            if (includes != null)
            {
                var builder = new IncludeBuilder<T>(conventions);
                includes(builder);

                var numberOfIncludesAdded = 0;

                if (builder.DocumentsToInclude?.Count > 0)
                {
                    queryBuilder
                        .AppendLine()
                        .Append("include ");

                    foreach (var inc in builder.DocumentsToInclude)
                    {
                        var include = "doc." + inc;

                        if (numberOfIncludesAdded > 0)
                            queryBuilder.Append(",");

                        if (IncludesUtil.RequiresQuotes(include, out var escapedInclude))
                        {
                            queryBuilder
                                .Append("'")
                                .Append(escapedInclude)
                                .Append("'");
                        }
                        else
                            queryBuilder.Append(include);

                        numberOfIncludesAdded++;
                    }
                }

                if (builder.AllCounters)
                {
                    if (numberOfIncludesAdded == 0)
                    {
                        queryBuilder
                            .AppendLine()
                            .Append("include ");
                    }

                    var token = CounterIncludesToken.All(string.Empty);
                    token.WriteTo(queryBuilder);

                    numberOfIncludesAdded++;
                }
                else if (builder.CountersToInclude?.Count > 0)
                {
                    if (numberOfIncludesAdded == 0)
                    {
                        queryBuilder
                            .AppendLine()
                            .Append("include ");
                    }

                    foreach (var counterName in builder.CountersToInclude)
                    {
                        if (numberOfIncludesAdded > 0)
                            queryBuilder.Append(",");

                        var token = CounterIncludesToken.Create(string.Empty, counterName);
                        token.WriteTo(queryBuilder);

                        numberOfIncludesAdded++;
                    }
                }

                if (builder.TimeSeriesToInclude != null)
                {
                    foreach (var timeSeriesRange in builder.TimeSeriesToInclude)
                    {
                        if (numberOfIncludesAdded == 0)
                        {
                            queryBuilder
                                .AppendLine()
                                .Append("include ");
                        }

                        if (numberOfIncludesAdded > 0)
                            queryBuilder.Append(",");

                        var token = TimeSeriesIncludesToken.Create(string.Empty, timeSeriesRange);
                        token.WriteTo(queryBuilder);

                        numberOfIncludesAdded++;
                    }
                }
            }

            criteria.Query = queryBuilder.ToString();
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
                var dispose = subscription.DisposeAsync();
                if (dispose.IsCompletedSuccessfully)
                    continue;

                tasks.Add(dispose.AsTask());
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

        public void Enable(string name, string database = null)
        {
            AsyncHelpers.RunSync(() => EnableAsync(name, database));
        }

        public Task EnableAsync(string name, string database = null, CancellationToken token = default)
        {
            var operation = new ToggleOngoingTaskStateOperation(name, OngoingTaskType.Subscription, disable: false);
            return _store.Maintenance.ForDatabase(database ?? _store.Database).SendAsync(operation, token);
        }

        public void Disable(string name, string database = null)
        {
            AsyncHelpers.RunSync(() => DisableAsync(name, database));
        }

        public Task DisableAsync(string name, string database = null, CancellationToken token = default)
        {
            var operation = new ToggleOngoingTaskStateOperation(name, OngoingTaskType.Subscription, disable: true);
            return _store.Maintenance.ForDatabase(database ?? _store.Database).SendAsync(operation, token);
        }

        /// <summary>
        /// Update a data subscription in a database.
        /// </summary>
        public string Update(SubscriptionUpdateOptions options, string database = null)
        {
            return AsyncHelpers.RunSync(() => UpdateAsync(options, database));
        }

        public async Task<string> UpdateAsync(SubscriptionUpdateOptions options, string database = null, CancellationToken token = default)
        {
            if (options == null)
                throw new ArgumentNullException($"Cannot update a subscription if {nameof(options)} is null");

            if (string.IsNullOrEmpty(options.Name) && options.Id == null)
                throw new ArgumentNullException($"Cannot update a subscription if both {nameof(options)}.{nameof(options.Name)} and {nameof(options)}.{nameof(options.Id)} are null");

            var requestExecutor = _store.GetRequestExecutor(database);
            using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var command = new UpdateSubscriptionCommand(options);
                await requestExecutor.ExecuteAsync(command, context, sessionInfo: null, token: token).ConfigureAwait(false);

                return command.Result.Name;
            }
        }
    }
}
