//-----------------------------------------------------------------------
// <copyright file="ShardedDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Document;
using Raven.Client.Document.Async;
using Raven.Client.Document.Batches;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Connection.Async;
using System.Threading.Tasks;
using Raven.Client.Connection;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Json.Linq;

namespace Raven.Client.Shard
{
    /// <summary>
    /// Implements Unit of Work for accessing a set of sharded RavenDB servers
    /// </summary>
    public class AsyncShardedDocumentSession : BaseShardedDocumentSession<IAsyncDatabaseCommands>,
                                               IAsyncDocumentSessionImpl, IAsyncAdvancedSessionOperations
    {
        private readonly AsyncDocumentKeyGeneration asyncDocumentKeyGeneration;

        public AsyncShardedDocumentSession(string dbName, ShardedDocumentStore documentStore, DocumentSessionListeners listeners, Guid id,
                                           ShardStrategy shardStrategy, IDictionary<string, IAsyncDatabaseCommands> shardDbCommands)
            : base(dbName, documentStore, listeners, id, shardStrategy, shardDbCommands)
        {
            GenerateDocumentKeysOnStore = false;
            asyncDocumentKeyGeneration = new AsyncDocumentKeyGeneration(this, entitiesAndMetadata.TryGetValue, ModifyObjectId);
        }

        protected override JsonDocument GetJsonDocument(string documentKey)
        {
            throw new NotSupportedException("This method requires a synchronous call to the server, which is not supported by the async session");
        }

        #region Properties to access different interfaces

        IAsyncAdvancedSessionOperations IAsyncDocumentSession.Advanced
        {
            get { return this; }
        }

        #endregion

        #region Load and Include

        public Lazy<Task<TResult[]>> LoadAsync<TResult>(IEnumerable<string> ids, Action<TResult[]> onEval, CancellationToken token = default (CancellationToken))
        {
            throw new NotImplementedException();
        }

        Lazy<Task<TResult>> IAsyncLazySessionOperations.LoadAsync<TResult>(string id, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public Lazy<Task<TResult>> LoadAsync<TResult>(string id, Action<TResult> onEval, CancellationToken token = default (CancellationToken))
        {
            throw new NotImplementedException();
        }

        Lazy<Task<TResult>> IAsyncLazySessionOperations.LoadAsync<TResult>(ValueType id, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public Lazy<Task<TResult>> LoadAsync<TResult>(ValueType id, Action<TResult> onEval, CancellationToken token = default (CancellationToken))
        {
            throw new NotImplementedException();
        }

        Lazy<Task<TResult[]>> IAsyncLazySessionOperations.LoadAsync<TResult>(CancellationToken token,params ValueType[] ids)
        {
            throw new NotImplementedException();
        }

        Lazy<Task<TResult[]>> IAsyncLazySessionOperations.LoadAsync<TResult>(IEnumerable<ValueType> ids, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public Lazy<Task<TResult[]>> LoadAsync<TResult>(IEnumerable<ValueType> ids, Action<TResult[]> onEval, CancellationToken token = default (CancellationToken))
        {
            throw new NotImplementedException();
        }

        Lazy<Task<TResult>> IAsyncLazySessionOperations.LoadAsync<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure, Action<TResult> onEval, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        Lazy<Task<TResult>> IAsyncLazySessionOperations.LoadAsync<TResult>(string id, Type transformerType, Action<ILoadConfiguration> configure, Action<TResult> onEval, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public IAsyncLazySessionOperations Lazily { get; private set; }
        public IAsyncEagerSessionOperations Eagerly { get; private set; }

        public Task<FacetResults[]> MultiFacetedSearchAsync(params FacetQuery[] queries)
        {
            throw new NotSupportedException("Multi faceted searching is currently not supported by async sharded document store");
        }

        public string GetDocumentUrl(object entity)
        {
            DocumentMetadata value;
            if (entitiesAndMetadata.TryGetValue(entity, out value) == false)
                throw new ArgumentException("The entity is not part of the session");

            var shardId = value.Metadata.Value<string>(Constants.RavenShardId);
            IAsyncDatabaseCommands commands;
            if (shardDbCommands.TryGetValue(shardId, out commands) == false)
                throw new InvalidOperationException("Could not find matching shard for shard id: " + shardId);
            return commands.UrlFor(value.Key);
        }

        Lazy<Task<TResult[]>> IAsyncLazySessionOperations.LoadStartingWithAsync<TResult>(string keyPrefix, string matches, int start, int pageSize, string exclude, RavenPagingInformation pagingInformation, string skipAfter, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public Lazy<Task<TResult[]>> MoreLikeThisAsync<TResult>(MoreLikeThisQuery query, CancellationToken token = default (CancellationToken))
        {
            throw new NotImplementedException();
        }

        public Task<T> LoadAsync<T>(string id, CancellationToken token = default (CancellationToken))
        {
            if (knownMissingIds.Contains(id))
            {
                return CompletedTask.With(default(T));
            }

            object existingEntity;
            if (entitiesByKey.TryGetValue(id, out existingEntity))
            {
                return CompletedTask.With((T)existingEntity);
            }
           

            IncrementRequestCount();
            var shardRequestData = new ShardRequestData
            {
                EntityType = typeof(T),
                Keys = { id }
            };

            var dbCommands = GetCommandsToOperateOn(shardRequestData);
            var results = shardStrategy.ShardAccessStrategy.ApplyAsync(dbCommands, shardRequestData, (commands, i) =>
            {
                var loadOperation = new LoadOperation(this, commands.DisableAllCaching, id);

                Func<Task> executer = null;
                executer = () =>
                {
                    loadOperation.LogOperation();

                    var loadContext = loadOperation.EnterLoadContext();
                    return commands.GetAsync(id, token).ContinueWith(task =>
                    {
                        if (loadContext != null)
                            loadContext.Dispose();

                        if (loadOperation.SetResult(task.Result))
                            return executer();
                        return new CompletedTask();
                    }).Unwrap();
                };
                return executer().ContinueWith(_ =>
                {
                    _.AssertNotFailed();
                    return loadOperation.Complete<T>();
                }).WithCancellation(token);
            });

            return results.ContinueWith(task =>
            {
                var shardsContainThisDocument = task.Result.Where(x => !Equals(x, default(T))).ToArray();
                if (shardsContainThisDocument.Count() > 1)
                {
                    throw new InvalidOperationException("Found document with id: " + id +
                                                        " on more than a single shard, which is not allowed. Document keys have to be unique cluster-wide.");
                }

                return shardsContainThisDocument.FirstOrDefault();
            }).WithCancellation(token);
        }

        IAsyncLazyLoaderWithInclude<TResult> IAsyncLazySessionOperations.Include<TResult>(Expression<Func<TResult, object>> path)
        {
            throw new NotImplementedException();
        }

        Lazy<Task<TResult[]>> IAsyncLazySessionOperations.LoadAsync<TResult>(IEnumerable<string> ids, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public Task<T[]> LoadAsync<T>(IEnumerable<string> ids, CancellationToken token = default (CancellationToken))
        {
            return LoadAsyncInternal<T>(ids.ToArray(), token);
        }

        public Task<T> LoadAsync<T>(ValueType id, CancellationToken token = default (CancellationToken))
        {
            var documentKey = Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false);
            return LoadAsync<T>(documentKey, token);
        }

        public Task<T[]> LoadAsync<T>(CancellationToken token = default (CancellationToken),params ValueType[] ids)
        {
            var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return LoadAsyncInternal<T>(documentKeys.ToArray(), token);
        }

        public Task<T[]> LoadAsync<T>(IEnumerable<ValueType> ids, CancellationToken token = default (CancellationToken))
        {
            var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return LoadAsyncInternal<T>(documentKeys.ToArray(), token);
        }

        public Task<T[]> LoadAsyncInternal<T>(string[] ids, CancellationToken token = default (CancellationToken))
        {
            return LoadAsyncInternal<T>(ids, null, token);
        }

        public async Task<T> LoadAsync<TTransformer, T>(string id, Action<ILoadConfiguration> configure = null, CancellationToken token = default (CancellationToken)) where TTransformer : AbstractTransformerCreationTask, new()
        {
            var configuration = new RavenLoadConfiguration();
            if (configure != null)
                configure(configuration);

            var result = await LoadUsingTransformerInternalAsync<T>(new[] { id }, null, new TTransformer().TransformerName, configuration.TransformerParameters, token).ConfigureAwait(false);
            return result.FirstOrDefault();
        }

        public async Task<TResult[]> LoadAsync<TTransformer, TResult>(IEnumerable<string> ids, Action<ILoadConfiguration> configure = null, CancellationToken token = default (CancellationToken)) where TTransformer : AbstractTransformerCreationTask, new()
        {
            var configuration = new RavenLoadConfiguration();
            if (configure != null)
                configure(configuration);

            var result = await LoadUsingTransformerInternalAsync<TResult>(ids.ToArray(), null, new TTransformer().TransformerName, configuration.TransformerParameters, token).ConfigureAwait(false);
            return result;
        }

        public async Task<TResult> LoadAsync<TResult>(string id, string transformer, Action<ILoadConfiguration> configure = null, CancellationToken token = default (CancellationToken))
        {
            var configuration = new RavenLoadConfiguration();
            if (configure != null)
                configure(configuration);

            var result = await LoadUsingTransformerInternalAsync<TResult>(new[] { id }, null, transformer, configuration.TransformerParameters, token).ConfigureAwait(false);
            return result.FirstOrDefault();
        }

        public async Task<TResult[]> LoadAsync<TResult>(IEnumerable<string> ids, string transformer, Action<ILoadConfiguration> configure = null, CancellationToken token = default (CancellationToken))
        {
            var configuration = new RavenLoadConfiguration();
            if (configure != null)
                configure(configuration);

            return await LoadUsingTransformerInternalAsync<TResult>(ids.ToArray(), null, transformer, configuration.TransformerParameters, token).ConfigureAwait(false);
        }

        public async Task<TResult> LoadAsync<TResult>(string id, Type transformerType, Action<ILoadConfiguration> configure = null, CancellationToken token = default (CancellationToken))
        {
            var configuration = new RavenLoadConfiguration();
            if (configure != null)
                configure(configuration);

            var transformer = ((AbstractTransformerCreationTask)Activator.CreateInstance(transformerType)).TransformerName;

            var result = await LoadUsingTransformerInternalAsync<TResult>(new[] { id }, null, transformer, configuration.TransformerParameters, token).ConfigureAwait(false);
            return result.FirstOrDefault();
        }

        public async Task<TResult[]> LoadAsync<TResult>(IEnumerable<string> ids, Type transformerType, Action<ILoadConfiguration> configure = null, CancellationToken token = default (CancellationToken))
        {
            var configuration = new RavenLoadConfiguration();
            if (configure != null)
                configure(configuration);

            var transformer = ((AbstractTransformerCreationTask)Activator.CreateInstance(transformerType)).TransformerName;

            var result = await LoadUsingTransformerInternalAsync<TResult>(ids.ToArray(), null, transformer, configuration.TransformerParameters, token).ConfigureAwait(false);
            return result;
        }

        public async Task<T[]> LoadAsyncInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes, CancellationToken token = default (CancellationToken))
        {
            var results = new T[ids.Length];
            var includePaths = includes != null ? includes.Select(x => x.Key).ToArray() : null;
            var idsToLoad = GetIdsThatNeedLoading<T>(ids, includePaths, transformer: null).ToList();

            if (!idsToLoad.Any())
                return results;

            IncrementRequestCount();

            foreach (var shard in idsToLoad)
            {
                var currentShardIds = shard.Select(x => x.Id).ToArray();
                var multiLoadOperations = await shardStrategy.ShardAccessStrategy.ApplyAsync(shard.Key, new ShardRequestData
                {
                    EntityType = typeof(T),
                    Keys = currentShardIds.ToList()
                }, async (dbCmd, i) =>
                {
                    var multiLoadOperation = new MultiLoadOperation(this, dbCmd.DisableAllCaching, currentShardIds, includes);
                    MultiLoadResult multiLoadResult;
                    do
                    {
                        multiLoadOperation.LogOperation();
                        using (multiLoadOperation.EnterMultiLoadContext())
                        {
                            multiLoadResult = await dbCmd.GetAsync(currentShardIds, includePaths, token: token).ConfigureAwait(false);
                        }
                    } while (multiLoadOperation.SetResult(multiLoadResult));
                    return multiLoadOperation;
                }).WithCancellation(token).ConfigureAwait(false);
                foreach (var multiLoadOperation in multiLoadOperations)
                {
                    token.ThrowIfCancellationRequested();
                    var loadResults = multiLoadOperation.Complete<T>();
                    for (int i = 0; i < loadResults.Length; i++)
                    {
                        if (ReferenceEquals(loadResults[i], null))
                            continue;
                        var id = currentShardIds[i];
                        var itemPosition = Array.IndexOf(ids, id);
                        if (ReferenceEquals(results[itemPosition], default(T)) == false)
                        {
                            throw new InvalidOperationException("Found document with id: " + id +
                                                                " on more than a single shard, which is not allowed. Document keys have to be unique cluster-wide.");
                        }
                        results[itemPosition] = loadResults[i];
                    }
                }
            }
            return ids.Select(id => // so we get items that were skipped because they are already in the session cache
            {
                object val;
                entitiesByKey.TryGetValue(id, out val);
                return (T)val;
            }).ToArray();
        }

        public async Task<T[]> LoadUsingTransformerInternalAsync<T>(string[] ids, KeyValuePair<string, Type>[] includes, string transformer, 
            Dictionary<string, RavenJToken> transformerParameters = null, CancellationToken token = default (CancellationToken))
        {
            token.ThrowIfCancellationRequested(); //if cancel already requested prevent incrementing request count
            var results = new T[ids.Length];
            var includePaths = includes != null ? includes.Select(x => x.Key).ToArray() : null;
            var idsToLoad = GetIdsThatNeedLoading<T>(ids, includePaths, transformer).ToList();

            if (!idsToLoad.Any())
                return results;

            IncrementRequestCount();

            if (typeof(T).IsArray)
            {
                foreach (var shard in idsToLoad)
                {
                    token.ThrowIfCancellationRequested();
                    var currentShardIds = shard.Select(x => x.Id).ToArray();
                    var shardResults = await shardStrategy.ShardAccessStrategy.ApplyAsync(shard.Key,
                        new ShardRequestData { EntityType = typeof(T), Keys = currentShardIds.ToList() },
                        async (dbCmd, i) =>
                        {
                            // Returns array of arrays, public APIs don't surface that yet though as we only support Transform
                            // With a single Id
                            var arrayOfArrays = (await dbCmd.GetAsync(currentShardIds, includePaths, transformer, transformerParameters, token: token).ConfigureAwait(false))
                                .Results
                                .Select(x => x.Value<RavenJArray>("$values").Cast<RavenJObject>())
                                .Select(values =>
                                {
                                    var array = values.Select(y =>
                                    {
                                        HandleInternalMetadata(y);
                                        return ConvertToEntity(typeof(T),null, y, new RavenJObject());
                                    }).ToArray();
                                    var newArray = Array.CreateInstance(typeof(T).GetElementType(), array.Length);
                                    Array.Copy(array, newArray, array.Length);
                                    return newArray;
                                })
                                .Cast<T>()
                                .ToArray();

                            return arrayOfArrays;
                        }).WithCancellation(token).ConfigureAwait(false);

                    return shardResults.SelectMany(x => x).ToArray();
                }
            }

            foreach (var shard in idsToLoad)
            {
                token.ThrowIfCancellationRequested();
                var currentShardIds = shard.Select(x => x.Id).ToArray();
                var shardResults = await shardStrategy.ShardAccessStrategy.ApplyAsync(shard.Key,
                        new ShardRequestData { EntityType = typeof(T), Keys = currentShardIds.ToList() },
                        async (dbCmd, i) =>
                        {
                            var multiLoadResult = await dbCmd.GetAsync(currentShardIds, includePaths, transformer, transformerParameters).ConfigureAwait(false);

                            var items = new LoadTransformerOperation(this, transformer, ids).Complete<T>(multiLoadResult);

                            if (items.Length > currentShardIds.Length)
                            {
                                throw new InvalidOperationException(
                                    String.Format(
                                        "A load was attempted with transformer {0}, and more than one item was returned per entity - please use {1}[] as the projection type instead of {1}",
                                        transformer,
                                        typeof(T).Name));
                            }

                            return items;
                        }).WithCancellation(token).ConfigureAwait(false);

                foreach (var shardResult in shardResults)
                {
                    for (int i = 0; i < shardResult.Length; i++)
                    {
                        token.ThrowIfCancellationRequested();
                        if (ReferenceEquals(shardResult[i], null))
                            continue;
                        var id = currentShardIds[i];
                        var itemPosition = Array.IndexOf(ids, id);
                        if (ReferenceEquals(results[itemPosition], default(T)) == false)
                            throw new InvalidOperationException("Found document with id: " + id + " on more than a single shard, which is not allowed. Document keys have to be unique cluster-wide.");

                        results[itemPosition] = shardResult[i];
                    }
                }
            }

            return results;
        }

        public Lazy<Task<T[]>> LazyAsyncLoadInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes, Action<T[]> onEval, CancellationToken token = default (CancellationToken))
        {
            throw new NotImplementedException();
        }

        public IAsyncLoaderWithInclude<object> Include(string path)
        {
            return new AsyncMultiLoaderWithInclude<object>(this).Include(path);
        }

        IAsyncLazyLoaderWithInclude<object> IAsyncLazySessionOperations.Include(string path)
        {
            throw new NotImplementedException();
        }

        public IAsyncLoaderWithInclude<T> Include<T>(Expression<Func<T, object>> path)
        {
            return new AsyncMultiLoaderWithInclude<T>(this).Include(path);
        }

        public IAsyncLoaderWithInclude<T> Include<T, TInclude>(Expression<Func<T, object>> path)
        {
            return new AsyncMultiLoaderWithInclude<T>(this).Include<TInclude>(path);
        }

        #endregion

        #region Queries

        public override RavenQueryInspector<T> CreateRavenQueryInspector<T>()
        {
            return new ShardedRavenQueryInspector<T>(shardStrategy,
                 null,
                 shardDbCommands.Values.ToList());
        }

        protected override IDocumentQuery<T> DocumentQueryGeneratorQuery<T>(string indexName, bool isMapReduce)
        {
            throw new NotSupportedException("The async sharded document store doesn't support synchronous operations");
        }

        protected override IAsyncDocumentQuery<T> DocumentQueryGeneratorAsyncQuery<T>(string indexName, bool isMapReduce)
        {
            return AsyncDocumentQuery<T>(indexName);
        }

        public Task<IEnumerable<T>> LoadStartingWithAsync<T>(string keyPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null, RavenPagingInformation pagingInformation = null, string skipAfter = null, CancellationToken token = default (CancellationToken))
        {
            IncrementRequestCount();
            var shards = GetCommandsToOperateOn(new ShardRequestData
            {
                EntityType = typeof(T),
                Keys = { keyPrefix }
            });

            return shardStrategy.ShardAccessStrategy.ApplyAsync(shards, new ShardRequestData
            {
                EntityType = typeof(T),
                Keys = { keyPrefix }
            }, (dbCmd, i) => dbCmd.StartsWithAsync(keyPrefix, matches, start, pageSize, exclude: exclude, skipAfter:skipAfter, token: token))
                                  .ContinueWith(task => (IEnumerable<T>)task.Result.SelectMany(x => x).Select(TrackEntity<T>).ToList())
                                  .WithCancellation(token);
        }

        public Task<IEnumerable<TResult>> LoadStartingWithAsync<TTransformer, TResult>(string keyPrefix, string matches = null, int start = 0, int pageSize = 25,
                                                            string exclude = null, RavenPagingInformation pagingInformation = null,
                                                            Action<ILoadConfiguration> configure = null, string skipAfter = null, CancellationToken token = default (CancellationToken)) where TTransformer : AbstractTransformerCreationTask, new()
        {
            var transformer = new TTransformer().TransformerName;

            var configuration = new RavenLoadConfiguration();
            if (configure != null)
            {
                configure(configuration);
            }

            IncrementRequestCount();
            var shards = GetCommandsToOperateOn(new ShardRequestData
            {
                EntityType = typeof(TResult),
                Keys = { keyPrefix }
            });
            var queryOperation = new QueryOperation(this, "Load/StartingWith", null, null, false, TimeSpan.Zero, null, null, false);

            return shardStrategy.ShardAccessStrategy.ApplyAsync(shards, new ShardRequestData
            {
                EntityType = typeof(TResult),
                Keys = { keyPrefix }
            }, (dbCmd, i) => dbCmd.StartsWithAsync(keyPrefix, matches, start, pageSize, exclude: exclude, transformer: transformer,
                                                         transformerParameters: configuration.TransformerParameters,
                                                         skipAfter: skipAfter, token: token))
                                .ContinueWith(task => (IEnumerable<TResult>)task.Result.SelectMany(x => x).Select(x=> queryOperation.Deserialize<TResult>(x.ToJson())).ToList())
                                .WithCancellation(token);
        }

        /// <summary>
        /// Queries the index specified by <typeparamref name="TIndexCreator"/> using lucene syntax.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
        /// <returns></returns>
        [Obsolete("Use AsyncDocumentQuery instead.")]
        public IAsyncDocumentQuery<T> AsyncLuceneQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
        {
            return AsyncDocumentQuery<T, TIndexCreator>();
        }

        /// <summary>
        /// Queries the index specified by <typeparamref name="TIndexCreator"/> using lucene syntax.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
        /// <returns></returns>
        public IAsyncDocumentQuery<T> AsyncDocumentQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var index = new TIndexCreator();

            return AsyncDocumentQuery<T>(index.IndexName, index.IsMapReduce);
        }

        [Obsolete("Use AsyncDocumentQuery instead.")]
        public IAsyncDocumentQuery<T> AsyncLuceneQuery<T>(string indexName, bool isMapReduce = false)
        {
            return AsyncDocumentQuery<T>(indexName, isMapReduce);
        }

        public IAsyncDocumentQuery<T> AsyncDocumentQuery<T>(string indexName, bool isMapReduce = false)
        {
            return new AsyncShardedDocumentQuery<T>(this, GetShardsToOperateOn, shardStrategy, indexName, null, null, theListeners.QueryListeners, isMapReduce);
        }

        [Obsolete("Use AsyncDocumentQuery instead.")]
        public IAsyncDocumentQuery<T> AsyncLuceneQuery<T>()
        {
            return AsyncDocumentQuery<T>();
        }

        public IAsyncDocumentQuery<T> AsyncDocumentQuery<T>()
        {
            return AsyncDocumentQuery<T>(GetDynamicIndexName<T>());
        }

        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncDocumentQuery<T> query, CancellationToken token = default (CancellationToken))
        {
            return StreamAsync(query, new Reference<QueryHeaderInformation>(), token);
        }

        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IQueryable<T> query, CancellationToken token = default (CancellationToken))
        {
            return StreamAsync(query, new Reference<QueryHeaderInformation>(), token);
        }

        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncDocumentQuery<T> query, Reference<QueryHeaderInformation> queryHeaderInformation, CancellationToken token = default (CancellationToken))
        {
            throw new NotSupportedException("Streams are currently not supported by sharded document store");
        }


        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IQueryable<T> query, Reference<QueryHeaderInformation> queryHeaderInformation, CancellationToken token = default (CancellationToken))
        {
            throw new NotSupportedException("Streams are currently not supported by sharded document store");
        }

        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(Etag fromEtag, int start = 0, int pageSize = Int32.MaxValue, RavenPagingInformation pagingInformation = null, string transformer = null, Dictionary<string, RavenJToken> transformerParameters = null, CancellationToken token = default (CancellationToken))
        {
            throw new NotSupportedException("Streams are currently not supported by sharded document store");
        }

        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(string startsWith, string matches = null, int start = 0, int pageSize = Int32.MaxValue, RavenPagingInformation pagingInformation = null, string skipAfter = null, string transformer = null, Dictionary<string, RavenJToken> transformerParameters = null, CancellationToken token = default (CancellationToken))
        {
            throw new NotSupportedException("Streams are currently not supported by sharded document store");
        }

        public async Task RefreshAsync<T>(T entity, CancellationToken token = default (CancellationToken))
        {
            DocumentMetadata value;
            if (entitiesAndMetadata.TryGetValue(entity, out value) == false)
                throw new InvalidOperationException("Cannot refresh a transient instance");
            IncrementRequestCount();

            var shardRequestData = new ShardRequestData
            {
                EntityType = typeof(T),
                Keys = { value.Key }
            };
            var dbCommands = GetCommandsToOperateOn(shardRequestData);

            var results = await shardStrategy.ShardAccessStrategy.ApplyAsync(dbCommands, shardRequestData, async (dbCmd, i) =>
            {
                var jsonDocument = await dbCmd.GetAsync(value.Key, token).ConfigureAwait(false);
                if (jsonDocument == null)
                    return false;

                RefreshInternal(entity, jsonDocument, value);
                return true;
            }).WithCancellation(token).ConfigureAwait(false);

            if (results.All(x => x == false))
            {
                throw new InvalidOperationException("Document '" + value.Key + "' no longer exists and was probably deleted");
            }
        }

        #endregion

        /// <summary>
        /// Saves all the changes to the Raven server.
        /// </summary>
        async Task IAsyncDocumentSession.SaveChangesAsync(CancellationToken token)
        {
            await asyncDocumentKeyGeneration.GenerateDocumentKeysForSaveChanges().ConfigureAwait(false);
            var cachingScope = EntityToJson.EntitiesToJsonCachingScope();
            try
            {
                var data = PrepareForSaveChanges();
                if (data.Commands.Count == 0 && deferredCommandsByShard.Count == 0)
                {
                    cachingScope.Dispose();
                    return ; // nothing to do here
                }

                IncrementRequestCount();
                LogBatch(data);

                // split by shards
                var saveChangesPerShard = await GetChangesToSavePerShardAsync(data).ConfigureAwait(false);

                var saveTasks = new Task<BatchResult[]>[saveChangesPerShard.Count];
                var saveChanges = new List<SaveChangesData>();
                // execute on all shards
                foreach (var shardAndObjects in saveChangesPerShard)
                {
                    token.ThrowIfCancellationRequested();
                    var shardId = shardAndObjects.Key;

                    IAsyncDatabaseCommands databaseCommands;
                    if (shardDbCommands.TryGetValue(shardId, out databaseCommands) == false)
                        throw new InvalidOperationException(
                            string.Format("ShardedDocumentStore cannot found a DatabaseCommands for shard id '{0}'.", shardId));

                    var localCopy = shardAndObjects.Value;
                    saveTasks[saveChanges.Count] =databaseCommands.BatchAsync(localCopy.Commands.ToArray(), data.Options );
                    saveChanges.Add(localCopy);
                }
                await Task.WhenAll(saveTasks).ConfigureAwait(false);
                for (int index = 0; index < saveTasks.Length; index++)
                {
                    var results = await saveTasks[index].ConfigureAwait(false);
                    UpdateBatchResults(results, saveChanges[index]);
                }
            }
            catch
            {
                cachingScope.Dispose();
                throw;
            }
                                             
        }


        protected async Task<Dictionary<string, SaveChangesData>> GetChangesToSavePerShardAsync(SaveChangesData data)
        {
            var saveChangesPerShard = CreateSaveChangesBatchPerShardFromDeferredCommands();

            for (int index = 0; index < data.Entities.Count; index++)
            {
                var entity = data.Entities[index];
                var metadata = await GetMetadataForAsync(entity).ConfigureAwait(false);
                var shardId = metadata.Value<string>(Constants.RavenShardId);
                if (shardId == null)
                    throw new InvalidOperationException("Cannot save a document when the shard id isn't defined. Missing Raven-Shard-Id in the metadata");
                var shardSaveChangesData = saveChangesPerShard.GetOrAdd(shardId);
                shardSaveChangesData.Entities.Add(entity);
                shardSaveChangesData.Commands.Add(data.Commands[index]);
            }
            return saveChangesPerShard;
        }


        protected override string GenerateKey(object entity)
        {
            throw new NotSupportedException("Cannot generated key synchronously in an async session");
        }

        protected override void RememberEntityForDocumentKeyGeneration(object entity)
        {
            asyncDocumentKeyGeneration.Add(entity);
        }

        protected override Task<string> GenerateKeyAsync(object entity)
        {
            var shardId = shardStrategy.ShardResolutionStrategy.MetadataShardIdFor(entity);
            IAsyncDatabaseCommands value;
            if (shardDbCommands.TryGetValue(shardId, out value) == false)
                throw new InvalidOperationException("Could not find shard: " + shardId);
            return Conventions.GenerateDocumentKeyAsync(dbName, value, entity);
        }

        public Task<ResponseTimeInformation> ExecuteAllPendingLazyOperationsAsync(CancellationToken token = default (CancellationToken))
        {
            throw new NotSupportedException("Async lazy requests are not supported for sharded store");
        }

        public async Task<RavenJObject> GetMetadataForAsync<T>(T instance)
        {
            var metadata = await GetDocumentMetadataAsync(instance).ConfigureAwait(false);
            return metadata.Metadata;
        }

        private async Task<DocumentMetadata> GetDocumentMetadataAsync<T>(T instance)
        {
            DocumentMetadata value;
            if (entitiesAndMetadata.TryGetValue(instance, out value) == false)
            {
                string id;
                if (GenerateEntityIdOnTheClient.TryGetIdFromInstance(instance, out id)
                    || (instance is IDynamicMetaObjectProvider &&
                       GenerateEntityIdOnTheClient.TryGetIdFromDynamic(instance, out id)))
                {
                    AssertNoNonUniqueInstance(instance, id);
                    var jsonDocument = await GetJsonDocumentAsync(id).ConfigureAwait(false);
                    value = GetDocumentMetadataValue(instance, id, jsonDocument);
                }
                else
                {
                    throw new InvalidOperationException("Could not find the document key for " + instance);
                }
            }
            return value;
        }

        /// <summary>
        /// Get the json document by key from the store
        /// </summary>
        private async Task<JsonDocument> GetJsonDocumentAsync(string documentKey)
        {
             var shardRequestData = new ShardRequestData
            {
                EntityType = typeof(object),
                Keys = { documentKey }
            };
            var dbCommands = GetCommandsToOperateOn(shardRequestData);

            var documents = await shardStrategy.ShardAccessStrategy.ApplyAsync(dbCommands,
                shardRequestData,
                (commands, i) => commands.GetAsync(documentKey)).ConfigureAwait(false);

            var document = documents.FirstOrDefault(x => x != null);
            if (document != null)
                return document;

            throw new InvalidOperationException("Document '" + documentKey + "' no longer exists and was probably deleted");
             
        }

        public async Task<Operation> DeleteByIndexAsync<T, TIndexCreator>(Expression<Func<T, bool>> expression) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return await DeleteByIndexAsync(indexCreator.IndexName, expression).ConfigureAwait(false);
        }

        private IRavenQueryable<T> QueryInternal<T>(IAsyncDatabaseCommands assyncDatabaseCommands, string indexName, bool isMapReduce = false)
        {
            var ravenQueryStatistics = new RavenQueryStatistics();
            var highlightings = new RavenQueryHighlightings();
            var ravenQueryInspector = new RavenQueryInspector<T>();
            var ravenQueryProvider = new RavenQueryProvider<T>(this, indexName, ravenQueryStatistics, highlightings, null, assyncDatabaseCommands, isMapReduce);
            ravenQueryInspector.Init(ravenQueryProvider,
                ravenQueryStatistics,
                highlightings,
                indexName,
                null,
                this, null, assyncDatabaseCommands, isMapReduce);
            return ravenQueryInspector;
        }

        public async Task<Operation> DeleteByIndexAsync<T>(string indexName, Expression<Func<T, bool>> expression)
        {
            var shards = GetCommandsToOperateOn(new ShardRequestData
            {
                EntityType = typeof(T),
                Keys = { indexName }
            });

            var operations = shardStrategy.ShardAccessStrategy.ApplyAsync(shards, new ShardRequestData
            {
                EntityType = typeof(T),
                Keys = { indexName }
            }, (dbCmd, i) =>
            {
                var query = QueryInternal<T>(dbCmd, indexName).Where(expression);
                var indexQuery = new IndexQuery()
                {
                    Query = query.ToString()
                };

                return dbCmd.DeleteByIndexAsync(indexName, indexQuery);
            });

            var result = await operations.ConfigureAwait(false);

            var shardOperation = new ShardsOperation(result);

            return shardOperation;
        }
    }
}
