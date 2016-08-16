//-----------------------------------------------------------------------
// <copyright file="ShardedDocumentStore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.CodeDom;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Changes;
using Raven.Client.Connection.Async;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Client.Indexes;

namespace Raven.Client.Shard
{
    /// <summary>
    /// Implements a sharded document store
    /// Hiding most sharding details behind this and the <see cref="ShardedDocumentSession"/> gives you the ability to use
    /// sharding without really thinking about this too much
    /// </summary>
    public class ShardedDocumentStore : DocumentStoreBase
    {
        /// <summary>
        /// Gets the shared operations headers.
        /// </summary>
        /// <value>The shared operations headers.</value>
        /// <exception cref="NotSupportedException"></exception>
        public override NameValueCollection SharedOperationsHeaders
        {
            get { throw new NotSupportedException("Sharded document store doesn't have a SharedOperationsHeaders. you need to explicitly use the shard instances to get access to the SharedOperationsHeaders"); }
            protected set { throw new NotSupportedException("Sharded document store doesn't have a SharedOperationsHeaders. you need to explicitly use the shard instances to get access to the SharedOperationsHeaders"); }
        }

        /// <summary>
        /// Whatever this instance has json request factory available
        /// </summary>
        public override bool HasJsonRequestFactory
        {
            get { return false; }
        }

        /// <summary>
        /// Get the <see cref="HttpJsonRequestFactory"/> for this store
        /// </summary>
        public override HttpJsonRequestFactory JsonRequestFactory
        {
            get { throw new NotSupportedException("Sharded document store doesn't have a JsonRequestFactory. you need to explicitly use the shard instances to get access to the JsonRequestFactory"); }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ShardedDocumentStore"/> class.
        /// </summary>
        /// <param name="shardStrategy">The shard strategy.</param>
        public ShardedDocumentStore(ShardStrategy shardStrategy)
        {
            if (shardStrategy == null)
                throw new ArgumentException("Must have shard strategy", "shardStrategy");

            this.ShardStrategy = shardStrategy;
        }

        public override Document.DocumentConvention Conventions
        {
            get
            {
                return ShardStrategy.Conventions;
            }
            set
            {
                ShardStrategy.Conventions = value;
            }
        }

        /// <summary>
        /// Gets or sets the identifier for this store.
        /// </summary>
        /// <value>The identifier.</value>
        public override string Identifier { get; set; }

        /// <summary>
        /// Called after dispose is completed
        /// </summary>
        public override event EventHandler AfterDispose;

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public override void Dispose()
        {
            ShardStrategy.Shards.ForEach(shard => shard.Value.Dispose());

            WasDisposed = true;

            var afterDispose = AfterDispose;
            if (afterDispose != null)
                afterDispose(this, EventArgs.Empty);
        }

        /// <summary>
        /// Gets the async database commands.
        /// </summary>
        /// <value>The async database commands.</value>
        public override IAsyncDatabaseCommands AsyncDatabaseCommands
        {
            get { throw new NotSupportedException("Sharded document store doesn't have a database commands. you need to explicitly use the shard instances to get access to the database commands"); }
        }

        /// <summary>
        /// Opens the async session.
        /// </summary>
        /// <returns></returns>
        public override IAsyncDocumentSession OpenAsyncSession()
        {
            return OpenAsyncSessionInternal(null, ShardStrategy.Shards.ToDictionary(x => x.Key, x => x.Value.AsyncDatabaseCommands));
        }

        /// <summary>
        /// Opens the async session.
        /// </summary>
        /// <returns></returns>
        public override IAsyncDocumentSession OpenAsyncSession(string databaseName)
        {
            return OpenAsyncSessionInternal(databaseName, ShardStrategy.Shards.ToDictionary(x => x.Key, x => x.Value.AsyncDatabaseCommands.ForDatabase(databaseName)));
        }

        /// <summary>
        /// Opens the async session with the specified options.
        /// </summary>
        public override IAsyncDocumentSession OpenAsyncSession(OpenSessionOptions sessionOptions)
        {
            return OpenAsyncSessionInternal(sessionOptions.Database, ShardStrategy.Shards.ToDictionary(x => x.Key, x => x.Value.AsyncDatabaseCommands.ForDatabase(sessionOptions.Database).With(sessionOptions.Credentials)));
        }

        private IAsyncDocumentSession OpenAsyncSessionInternal(string dbName,Dictionary<string, IAsyncDatabaseCommands> shardDbCommands)
        {
            EnsureNotClosed();

            var sessionId = Guid.NewGuid();
            var session = new AsyncShardedDocumentSession(dbName, this, Listeners, sessionId, ShardStrategy, shardDbCommands);
            AfterSessionCreated(session);
            return session;
        }

        private readonly AtomicDictionary<IDatabaseChanges> changes =
            new AtomicDictionary<IDatabaseChanges>(StringComparer.OrdinalIgnoreCase);

        public override IDatabaseChanges Changes(string database = null)
        {
            return changes.GetOrAdd(database, 
                _ => new ShardedDatabaseChanges(ShardStrategy.Shards.Values.Select(x => x.Changes(database)).ToArray()));
        }

        /// <summary>
        /// Setup the context for aggressive caching.
        /// </summary>
        /// <param name="cacheDuration">Specify the aggressive cache duration</param>
        /// <remarks>
        /// aggressive caching means that we will not check the server to see whatever the response
        /// we provide is current or not, but will serve the information directly from the local cache
        /// without touching the server.
        /// </remarks>
        public override IDisposable AggressivelyCacheFor(TimeSpan cacheDuration)
        {
            var disposables =
                ShardStrategy.Shards.Select(shard => shard.Value.AggressivelyCacheFor(cacheDuration)).ToList();

            return new DisposableAction(() =>
            {
                foreach (var disposable in disposables)
                {
                    disposable.Dispose();
                }
            });
        }

        /// <summary>
        /// Setup the context for no aggressive caching
        /// </summary>
        /// <remarks>
        /// This is mainly useful for internal use inside RavenDB, when we are executing
        /// queries that has been marked with WaitForNonStaleResults, we temporarily disable
        /// aggressive caching.
        /// </remarks>
        public override IDisposable DisableAggressiveCaching()
        {
            var disposables = ShardStrategy.Shards.Select(shard => shard.Value.DisableAggressiveCaching()).ToList();

            return new DisposableAction(() =>
            {
                foreach (var disposable in disposables)
                {
                    disposable.Dispose();
                }
            });
        }

        /// <summary>
        /// Setup the WebRequest timeout for the session
        /// </summary>
        /// <param name="timeout">Specify the timeout duration</param>
        /// <remarks>
        /// Sets the timeout for the JsonRequest.  Scoped to the Current Thread.
        /// </remarks>
        public override IDisposable SetRequestsTimeoutFor(TimeSpan timeout) {
            var disposables =
                ShardStrategy.Shards.Select(shard => shard.Value.SetRequestsTimeoutFor(timeout)).ToList();

            return new DisposableAction(() => {
                foreach (var disposable in disposables) {
                    disposable.Dispose();
                }
            });
        }

        /// <summary>
        /// Opens the session.
        /// </summary>
        /// <returns></returns>
        public override IDocumentSession OpenSession()
        {
            return OpenSessionInternal(null, ShardStrategy.Shards.ToDictionary(x => x.Key, x => x.Value.DatabaseCommands));
        }

        /// <summary>
        /// Opens the session for a particular database
        /// </summary>
        public override IDocumentSession OpenSession(string database)
        {
            return OpenSessionInternal(database, ShardStrategy.Shards.ToDictionary(x => x.Key, x => x.Value.DatabaseCommands.ForDatabase(database)));
        }

        /// <summary>
        /// Opens the session with the specified options.
        /// </summary>
        public override IDocumentSession OpenSession(OpenSessionOptions sessionOptions)
        {
            return OpenSessionInternal(sessionOptions.Database, ShardStrategy.Shards.ToDictionary(x => x.Key, x => x.Value.DatabaseCommands
                .ForDatabase(sessionOptions.Database)
                .With(sessionOptions.Credentials)));
        }

        private IDocumentSession OpenSessionInternal(string database, Dictionary<string, IDatabaseCommands> shardDbCommands)
        {
            EnsureNotClosed();

            var sessionId = Guid.NewGuid();
            var session = new ShardedDocumentSession(database, this, null, Listeners, sessionId, ShardStrategy, shardDbCommands);
            AfterSessionCreated(session);
            return session;
        }

        /// <summary>
        /// Gets the database commands.
        /// </summary>
        /// <value>The database commands.</value>
        public override IDatabaseCommands DatabaseCommands
        {
            get { throw new NotSupportedException("Sharded document store doesn't have a database commands. you need to explicitly use the shard instances to get access to the database commands"); }
        }

        /// <summary>
        /// Gets or sets the URL.
        /// </summary>
        public override string Url
        {
            get { throw new NotSupportedException("There isn't a singular url when using sharding"); }
        }

        public ShardStrategy ShardStrategy { get; private set; }

        ///<summary>
        /// Gets the etag of the last document written by any session belonging to this 
        /// document store
        ///</summary>
        public override long? GetLastWrittenEtag()
        {
            throw new NotSupportedException("This isn't a single last written etag when sharding");
        }

        public override BulkInsertOperation BulkInsert(string database = null)
        {
            throw new NotSupportedException("Cannot use BulkInsert using Sharded store, use ShardedBulkInsert, instead");
        }

        public ShardedBulkInsertOperation ShardedBulkInsert(string database = null, ShardedDocumentStore store = null)
        {
            return new ShardedBulkInsertOperation(database, this);
        }

        public override void InitializeProfiling()
        {
            ShardStrategy.Shards.ForEach(shard => shard.Value.InitializeProfiling());
        }

        /// <summary>
        /// Initializes this instance.
        /// </summary>
        /// <returns></returns>
        public override IDocumentStore Initialize()
        {
            try
            {
                ShardStrategy.Shards.ForEach(shard => shard.Value.Initialize());

                var shardsPointingToSameDb = ShardStrategy.Shards
                    .GroupBy(x =>
                    {
                        try
                        {
                            return x.Value.DatabaseCommands.GetStatistics().DatabaseId;
                        }
                        catch (Exception)
                        {
                            return Guid.NewGuid();// we'll just ignore any connection erros here
                        }
                    }).FirstOrDefault(x => x.Count() > 1);


                if (shardsPointingToSameDb != null)
                    throw new NotSupportedException(string.Format("Multiple keys in shard dictionary for {0} are not supported.",
                        string.Join(", ", shardsPointingToSameDb.Select(x => x.Key))));

                if (Conventions.DocumentKeyGenerator == null)// don't overwrite what the user is doing
                {
                    var generator = new ShardedHiloKeyGenerator(this, 32);
                    Conventions.DocumentKeyGenerator = (dbName, commands, entity) => generator.GenerateDocumentKey(commands, Conventions, entity);
                }

                if (Conventions.AsyncDocumentKeyGenerator == null)
                {
                    var generator = new AsyncShardedHiloKeyGenerator(this, 32);
                    Conventions.AsyncDocumentKeyGenerator = (dbName, commands, entity) => generator.GenerateDocumentKeyAsync(commands, Conventions, entity);
                }
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }

            return this;
        }

        public IDatabaseCommands DatabaseCommandsFor(string shardId)
        {
            IDocumentStore store;
            if (ShardStrategy.Shards.TryGetValue(shardId, out store) == false)
                throw new InvalidOperationException("Could not find a shard named: " + shardId);

            return store.DatabaseCommands;
        }

        public IAsyncDatabaseCommands AsyncDatabaseCommandsFor(string shardId)
        {
            IDocumentStore store;
            if (ShardStrategy.Shards.TryGetValue(shardId, out store) == false)
                throw new InvalidOperationException("Could not find a shard named: " + shardId);

            return store.AsyncDatabaseCommands;
        }

        /// <summary>
        /// Executes the transformer creation
        /// </summary>
        public override void ExecuteTransformer(AbstractTransformerCreationTask transformerCreationTask)
        {
            var list = ShardStrategy.Shards.Values.Select(x => x.DatabaseCommands).ToList();
            ShardStrategy.ShardAccessStrategy.Apply(list,
                                                            new ShardRequestData()
                                                            , (commands, i) =>
                                                            {
                                                                transformerCreationTask.Execute(commands, Conventions);
                                                                return (object)null;
                                                            });
        }

        /// <summary>
        /// Executes the index creation against each of the shards.
        /// </summary>
        public override void ExecuteIndex(AbstractIndexCreationTask indexCreationTask)
        {
            var list = ShardStrategy.Shards.Values.Select(x => x.DatabaseCommands).ToList();
            ShardStrategy.ShardAccessStrategy.Apply(list,
                                                            new ShardRequestData()
                                                            , (commands, i) =>
                                                            {
                                                                indexCreationTask.Execute(commands, Conventions);
                                                                return (object)null;
                                                            });
        }

        public override void ExecuteIndexes(IList<AbstractIndexCreationTask> indexCreationTasks)
        {
            foreach (var store in ShardStrategy.Shards.Values)
                store.ExecuteIndexes(indexCreationTasks);
        }

        /// <summary>
        /// Executes the index creation against each of the shards Async.
        /// </summary>
        public override Task ExecuteIndexAsync(AbstractIndexCreationTask indexCreationTask)
        {
            var list = ShardStrategy.Shards.Values.Select(x => x.AsyncDatabaseCommands).ToList();
            return ShardStrategy.ShardAccessStrategy.ApplyAsync(list,new ShardRequestData(), (commands, i) =>
            {
                var tcs = new TaskCompletionSource<bool>();

                try
                {
                    indexCreationTask.ExecuteAsync(commands, Conventions)
                                     .ContinueWith(t => tcs.SetResult(true));
                }
                catch (Exception e)
                {
                    tcs.SetException(e);
                }

                return tcs.Task;
            });
        }

        public override async Task ExecuteIndexesAsync(List<AbstractIndexCreationTask> indexCreationTasks)
        {
            foreach (var store in ShardStrategy.Shards.Values)
                await store.ExecuteIndexesAsync(indexCreationTasks).ConfigureAwait(false);
        }

        public override void SideBySideExecuteIndexes(IList<AbstractIndexCreationTask> indexCreationTasks, long? minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            foreach (var store in ShardStrategy.Shards.Values)
                store.SideBySideExecuteIndexes(indexCreationTasks, minimumEtagBeforeReplace, replaceTimeUtc);
        }

        public override async Task SideBySideExecuteIndexesAsync(List<AbstractIndexCreationTask> indexCreationTasks, long? minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            foreach (var store in ShardStrategy.Shards.Values)
                await store.SideBySideExecuteIndexesAsync(indexCreationTasks, minimumEtagBeforeReplace, replaceTimeUtc).ConfigureAwait(false);
        }

        public override void SideBySideExecuteIndex(AbstractIndexCreationTask indexCreationTask, long? minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            var list = ShardStrategy.Shards.Values.Select(x => x.DatabaseCommands).ToList();
            ShardStrategy.ShardAccessStrategy.Apply(list,
                                                            new ShardRequestData()
                                                            , (commands, i) =>
                                                            {
                                                                indexCreationTask.SideBySideExecute(commands, Conventions, minimumEtagBeforeReplace, replaceTimeUtc);
                                                                return (object)null;
                                                            });
        }

        public override Task SideBySideExecuteIndexAsync(AbstractIndexCreationTask indexCreationTask, long? minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            var list = ShardStrategy.Shards.Values.Select(x => x.AsyncDatabaseCommands).ToList();
            return ShardStrategy.ShardAccessStrategy.ApplyAsync(list, new ShardRequestData(), (commands, i) =>
            {
                var tcs = new TaskCompletionSource<bool>();

                try
                {
                    indexCreationTask.SideBySideExecuteAsync(commands, Conventions, minimumEtagBeforeReplace, replaceTimeUtc)
                                     .ContinueWith(t => tcs.SetResult(true));
                }
                catch (Exception e)
                {
                    tcs.SetException(e);
                }

                return tcs.Task;
            });
        }

        public override Task ExecuteTransformerAsync(AbstractTransformerCreationTask transformerCreationTask)
        {
            var list = ShardStrategy.Shards.Values.Select(x => x.AsyncDatabaseCommands).ToList();
            return ShardStrategy.ShardAccessStrategy.ApplyAsync(list,
                                                            new ShardRequestData()
                                                            , (commands, i) =>
                                                            {
                                                                var tcs = new TaskCompletionSource<bool>();

                                                                try
                                                                {
                                                                    transformerCreationTask.ExecuteAsync(commands, Conventions)
                                                                        .ContinueWith(t => tcs.SetResult(true));
                                                                }
                                                                catch (Exception e)
                                                                {
                                                                    tcs.SetException(e);
                                                                }

                                                                return tcs.Task;
                                                            });
        }

        public bool WaitForNonStaleIndexesOnAllShards(TimeSpan? timeout = null)
        {
            foreach (var kvp in ShardStrategy.Shards)
            {
                var docStore = kvp.Value;
                TimeSpan? spinTimeout = timeout ?? (Debugger.IsAttached
              ? TimeSpan.FromMinutes(5)
              : TimeSpan.FromSeconds(20));
                if (SpinWait.SpinUntil(() => docStore.DatabaseCommands.GetStatistics().StaleIndexes.Length == 0, spinTimeout.Value) == false)
                    return false;
            }
            return true;
        }
    }
}
