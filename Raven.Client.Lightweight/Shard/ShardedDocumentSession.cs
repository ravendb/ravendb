//-----------------------------------------------------------------------
// <copyright file="ShardedDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Reflection;
using System.Threading;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Json.Linq;
using Raven.Client.Connection.Async;
using Raven.Client.Document.Batches;

namespace Raven.Client.Shard
{
    /// <summary>
    /// Implements Unit of Work for accessing a set of sharded RavenDB servers
    /// </summary>
    public class ShardedDocumentSession : BaseShardedDocumentSession<IDatabaseCommands>, IDocumentQueryGenerator,
                                          IDocumentSessionImpl, ISyncAdvancedSessionOperation
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ShardedDocumentSession"/> class.
        /// </summary>
        /// <param name="shardStrategy">The shard strategy.</param>
        /// <param name="shardDbCommands">The shard IDatabaseCommands.</param>
        /// <param name="id"></param>
        /// <param name="dbName">The db name.</param>
        /// <param name="documentStore"></param>
        /// <param name="listeners"></param>
        public ShardedDocumentSession(string dbName, ShardedDocumentStore documentStore, DocumentSessionListeners listeners, Guid id,
                                      ShardStrategy shardStrategy, IDictionary<string, IDatabaseCommands> shardDbCommands)
            : base(dbName, documentStore, listeners, id, shardStrategy, shardDbCommands) { }

        protected override JsonDocument GetJsonDocument(string documentKey)
        {
            var shardRequestData = new ShardRequestData
            {
                EntityType = typeof(object),
                Keys = { documentKey }
            };
            var dbCommands = GetCommandsToOperateOn(shardRequestData);

            var documents = shardStrategy.ShardAccessStrategy.Apply(dbCommands,
                                                                    shardRequestData,
                                                                    (commands, i) => commands.Get(documentKey));

            var document = documents.FirstOrDefault(x => x != null);
            if (document != null)
                return document;

            throw new InvalidOperationException("Document '" + documentKey + "' no longer exists and was probably deleted");
        }

        protected override string GenerateKey(object entity)
        {
            var shardId = shardStrategy.ShardResolutionStrategy.MetadataShardIdFor(entity);
            IDatabaseCommands value;
            if (shardDbCommands.TryGetValue(shardId, out value) == false)
                throw new InvalidOperationException("Could not find shard: " + shardId);
            return Conventions.GenerateDocumentKey(dbName, value, entity);
        }

        protected override Task<string> GenerateKeyAsync(object entity)
        {
            throw new NotSupportedException("Cannot generate key asynchronously using synchronous session");
        }

        #region Properties to access different interfacess

        ISyncAdvancedSessionOperation IDocumentSession.Advanced
        {
            get { return this; }
        }

        /// <summary>
        /// Access the lazy operations
        /// </summary>
        public ILazySessionOperations Lazily
        {
            get { return this; }
        }

        /// <summary>
        /// Access the eager operations
        /// </summary>
        IEagerSessionOperations ISyncAdvancedSessionOperation.Eagerly
        {
            get { return this; }
        }

        #endregion

        #region Load and Include

        #region Synchronous

        public TResult Load<TTransformer, TResult>(string id) where TTransformer : AbstractTransformerCreationTask, new()
        {
            var transformer = new TTransformer().TransformerName;
            return LoadInternal<TResult>(new[] { id }, null, transformer).FirstOrDefault();
        }

        public TResult Load<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure) where TTransformer : AbstractTransformerCreationTask, new()
        {
            var transformer = new TTransformer().TransformerName;
            var configuration = new RavenLoadConfiguration();
            if (configure != null)
            configure(configuration);
            return LoadInternal<TResult>(new[] { id }, null, transformer, configuration.TransformerParameters).FirstOrDefault();
        }

        public T Load<T>(string id)
        {
            if (IsDeleted(id))
                return default(T);

            object existingEntity;
            if (entitiesByKey.TryGetValue(id, out existingEntity))
            {
                return (T)existingEntity;
            }
            JsonDocument value;
            if (includedDocumentsByKey.TryGetValue(id, out value))
            {
                includedDocumentsByKey.Remove(id);
                return TrackEntity<T>(value);
            }
            IncrementRequestCount();
            var shardRequestData = new ShardRequestData
            {
                EntityType = typeof(T),
                Keys = { id }
            };
            var dbCommands = GetCommandsToOperateOn(shardRequestData);
            var results = shardStrategy.ShardAccessStrategy.Apply(dbCommands, shardRequestData, (commands, i) =>
            {
                var loadOperation = new LoadOperation(this, commands.DisableAllCaching, id);
                bool retry;
                do
                {
                    loadOperation.LogOperation();
                    using (loadOperation.EnterLoadContext())
                    {
                        retry = loadOperation.SetResult(commands.Get(id));
                    }
                } while (retry);
                return loadOperation.Complete<T>();
            });

            var shardsContainThisDocument = results.Where(x => !Equals(x, default(T))).ToArray();
            if (shardsContainThisDocument.Count() > 1)
            {
                throw new InvalidOperationException("Found document with id: " + id +
                                                    " on more than a single shard, which is not allowed. Document keys have to be unique cluster-wide.");
            }

            return shardsContainThisDocument.FirstOrDefault();
        }

        public T[] Load<T>(IEnumerable<string> ids)
        {
            return LoadInternal<T>(ids.ToArray());
        }

        public T Load<T>(ValueType id)
        {
            var documentKey = Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false);
            return Load<T>(documentKey);
        }

        public T[] Load<T>(params ValueType[] ids)
        {
            var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return Load<T>(documentKeys);
        }

        public T[] Load<T>(IEnumerable<ValueType> ids)
        {
            var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return Load<T>(documentKeys);
        }

        public TResult[] Load<TTransformer, TResult>(IEnumerable<string> ids, Action<ILoadConfiguration> configure = null) where TTransformer : AbstractTransformerCreationTask, new()
        {
            var configuration = new RavenLoadConfiguration();
            if (configure != null)
                configure(configuration);

            return LoadInternal<TResult>(ids.ToArray(), null, new TTransformer().TransformerName, configuration.TransformerParameters);
        }

        public TResult Load<TResult>(string id, string transformer, Action<ILoadConfiguration> configure = null)
        {
            var configuration = new RavenLoadConfiguration();
            if (configure != null)
                configure(configuration);

            return LoadInternal<TResult>(new[] { id }, null, transformer, configuration.TransformerParameters).FirstOrDefault();
        }

        public TResult[] Load<TResult>(IEnumerable<string> ids, string transformer, Action<ILoadConfiguration> configure = null)
        {
            var configuration = new RavenLoadConfiguration();
            if (configure != null)
                configure(configuration);

            return LoadInternal<TResult>(ids.ToArray(), null, transformer, configuration.TransformerParameters);
        }

        public TResult Load<TResult>(string id, Type transformerType, Action<ILoadConfiguration> configure = null)
        {
            var configuration = new RavenLoadConfiguration();
            if (configure != null)
                configure(configuration);

            var transformer = ((AbstractTransformerCreationTask)Activator.CreateInstance(transformerType)).TransformerName;

            return LoadInternal<TResult>(new[] { id }, null, transformer, configuration.TransformerParameters).FirstOrDefault();
        }

        public TResult[] Load<TResult>(IEnumerable<string> ids, Type transformerType, Action<ILoadConfiguration> configure = null)
        {
            var configuration = new RavenLoadConfiguration();
            if (configure != null)
                configure(configuration);

            var transformer = ((AbstractTransformerCreationTask)Activator.CreateInstance(transformerType)).TransformerName;

            return LoadInternal<TResult>(ids.ToArray(), null, transformer, configuration.TransformerParameters);
        }

        public T[] LoadInternal<T>(string[] ids, string transformer, Dictionary<string, RavenJToken> transformerParameters = null)
        {
            return LoadInternal<T>(ids.ToArray(), null, transformer, transformerParameters);
        }

        public T[] LoadInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes, string transformer, Dictionary<string, RavenJToken> transformerParameters = null)
        {
            var results = new T[ids.Length];
            var includePaths = includes != null ? includes.Select(x => x.Key).ToArray() : null;

            var idsToLoad = GetIdsThatNeedLoading<T>(ids, includePaths, transformer).ToList();

            if (idsToLoad.Count == 0)
                return results;

            IncrementRequestCount();

            if (typeof(T).IsArray)
            {
                foreach (var shard in idsToLoad)
                {
                    var currentShardIds = shard.Select(x => x.Id).ToArray();
                    var shardResults = shardStrategy.ShardAccessStrategy.Apply(shard.Key,
                            new ShardRequestData { EntityType = typeof(T), Keys = currentShardIds.ToList() },
                            (dbCmd, i) =>
                            {
                                // Returns array of arrays, public APIs don't surface that yet though as we only support Transform
                                // With a single Id
                                var arrayOfArrays = (dbCmd.Get(currentShardIds, includePaths, transformer, transformerParameters))
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
                            });

                    return shardResults.SelectMany(x => x).ToArray();
                }
            }

            foreach (var shard in idsToLoad)
            {
                var currentShardIds = shard.Select(x => x.Id).ToArray();
                var shardResults = shardStrategy.ShardAccessStrategy.Apply(shard.Key,
                        new ShardRequestData { EntityType = typeof(T), Keys = currentShardIds.ToList() },
                        (dbCmd, i) =>
                        {
                            var multiLoadResult = dbCmd.Get(currentShardIds, includePaths, transformer, transformerParameters);
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
                        });

                foreach (var shardResult in shardResults)
                {
                    for (int i = 0; i < shardResult.Length; i++)
                    {
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

        public T[] LoadInternal<T>(string[] ids)
        {
            return LoadInternal<T>(ids, null);
        }

        public T[] LoadInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes)
        {
            var results = new T[ids.Length];
            var includePaths = includes != null && includes.Length > 0 ? includes.Select(x => x.Key).ToArray() : null;
            var idsToLoad = GetIdsThatNeedLoading<T>(ids, includePaths, transformer: null).ToList();

            if (idsToLoad.Count>0)
                IncrementRequestCount();

            foreach (var shard in idsToLoad)
            {
                var currentShardIds = shard.Select(x => x.Id).ToArray();
                var multiLoadOperations = shardStrategy.ShardAccessStrategy.Apply(shard.Key, new ShardRequestData
                {
                    EntityType = typeof(T),
                    Keys = currentShardIds.ToList()
                }, (dbCmd, i) =>
                {
                    var multiLoadOperation = new MultiLoadOperation(this, dbCmd.DisableAllCaching, currentShardIds, includes);
                    MultiLoadResult multiLoadResult;
                    do
                    {
                        multiLoadOperation.LogOperation();
                        using (multiLoadOperation.EnterMultiLoadContext())
                        {
                            multiLoadResult = dbCmd.Get(currentShardIds, includePaths);
                        }
                    } while (multiLoadOperation.SetResult(multiLoadResult));
                    return multiLoadOperation;
                });
                foreach (var multiLoadOperation in multiLoadOperations)
                {
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

        public ILoaderWithInclude<object> Include(string path)
        {
            return new MultiLoaderWithInclude<object>(this).Include(path);
        }

        public ILoaderWithInclude<T> Include<T>(Expression<Func<T, object>> path)
        {
            return new MultiLoaderWithInclude<T>(this).Include(path);
        }

        public ILoaderWithInclude<T> Include<T, TInclude>(Expression<Func<T, object>> path)
        {
            return new MultiLoaderWithInclude<T>(this).Include<TInclude>(path);
        }


        #endregion

        #region Lazy loads

        /// <summary>
        /// Loads the specified ids and a function to call when it is evaluated
        /// </summary>
        Lazy<T[]> ILazySessionOperations.Load<T>(IEnumerable<string> ids, Action<T[]> onEval)
        {
            return LazyLoadInternal(ids.ToArray(), new KeyValuePair<string, Type>[0], onEval);
        }

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        Lazy<T[]> ILazySessionOperations.Load<T>(IEnumerable<string> ids)
        {
            return Lazily.Load<T>(ids, null);
        }

        /// <summary>
        /// Loads the specified id.
        /// </summary>
        Lazy<T> ILazySessionOperations.Load<T>(string id)
        {
            return Lazily.Load(id, (Action<T>)null);
        }

        /// <summary>
        /// Loads the specified id and a function to call when it is evaluated
        /// </summary>
        Lazy<T> ILazySessionOperations.Load<T>(string id, Action<T> onEval)
        {
            var cmds = GetCommandsToOperateOn(new ShardRequestData
            {
                Keys = { id },
                EntityType = typeof(T)
            });

            var lazyLoadOperation = new LazyLoadOperation<T>(id, new LoadOperation(this, () =>
            {
                var list = cmds.Select(databaseCommands => databaseCommands.DisableAllCaching()).ToList();
                return new DisposableAction(() => list.ForEach(x => x.Dispose()));
            }, id), HandleInternalMetadata);
            return AddLazyOperation(lazyLoadOperation, onEval, cmds);
        }

        internal Lazy<T> AddLazyOperation<T>(ILazyOperation operation, Action<T> onEval, IList<IDatabaseCommands> cmds)
        {
            pendingLazyOperations.Add(Tuple.Create(operation, cmds));
            var lazyValue = new Lazy<T>(() =>
            {
                ExecuteAllPendingLazyOperations();
                return (T)operation.Result;
            });
            if (onEval != null)
                onEvaluateLazy[operation] = result => onEval((T)result);

            return lazyValue;
        }

        internal Lazy<int> AddLazyCountOperation(ILazyOperation operation, IList<IDatabaseCommands> cmds)
        {
            pendingLazyOperations.Add(Tuple.Create(operation, cmds));
            var lazyValue = new Lazy<int>(() =>
            {
                ExecuteAllPendingLazyOperations();
                return operation.QueryResult.TotalResults;
            });

            return lazyValue;
        }

        /// <summary>
        /// Loads the specified entity with the specified id after applying
        /// conventions on the provided id to get the real document id.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// Load{Post}(1)
        /// And that call will internally be translated to 
        /// Load{Post}("posts/1");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        Lazy<T> ILazySessionOperations.Load<T>(ValueType id)
        {
            return Lazily.Load(id, (Action<T>)null);
        }

        /// <summary>
        /// Loads the specified entity with the specified id after applying
        /// conventions on the provided id to get the real document id.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// Load{Post}(1)
        /// And that call will internally be translated to 
        /// Load{Post}("posts/1");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        Lazy<T> ILazySessionOperations.Load<T>(ValueType id, Action<T> onEval)
        {
            var documentKey = Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false);
            return Lazily.Load(documentKey, onEval);
        }

        Lazy<T[]> ILazySessionOperations.Load<T>(IEnumerable<ValueType> ids)
        {
            var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return Lazily.Load<T>(documentKeys, null);
        }

        Lazy<T[]> ILazySessionOperations.Load<T>(IEnumerable<ValueType> ids, Action<T[]> onEval)
        {
            var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return Lazily.Load(documentKeys, onEval);
        }

        Lazy<TResult> ILazySessionOperations.Load<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure, Action<TResult> onEval)
        {
            var lazy = Lazily.Load<TTransformer, TResult>(new[] { id }, configure);
            return new Lazy<TResult>(() => lazy.Value[0]);
        }

        Lazy<TResult> ILazySessionOperations.Load<TResult>(string id, Type transformerType, Action<ILoadConfiguration> configure, Action<TResult> onEval)
        {
            var lazy = Lazily.Load(new[] { id }, transformerType, configure, onEval);
            return new Lazy<TResult>(() => lazy.Value[0]);
        }

        Lazy<TResult[]> ILazySessionOperations.Load<TTransformer, TResult>(IEnumerable<string> ids, Action<ILoadConfiguration> configure, Action<TResult> onEval)
        {
            return Lazily.Load(ids, typeof(TTransformer), configure, onEval);
        }

        Lazy<TResult[]> ILazySessionOperations.Load<TResult>(IEnumerable<string> ids, Type transformerType, Action<ILoadConfiguration> configure, Action<TResult> onEval)
        {
            var idsArray = ids.ToArray();
            var cmds = GetCommandsToOperateOn(new ShardRequestData
            {
                Keys = idsArray,
                EntityType = transformerType
            });

            var transformer = ((AbstractTransformerCreationTask)Activator.CreateInstance(transformerType)).TransformerName;
            var op = new LoadTransformerOperation(this, transformer, idsArray);

            var configuration = new RavenLoadConfiguration();
            if (configure != null)
                configure(configuration);

            var lazyLoadOperation = new LazyTransformerLoadOperation<TResult>(idsArray, transformer, configuration.TransformerParameters, op, false);

            return AddLazyOperation<TResult[]>(lazyLoadOperation, null, cmds);
        }

        Lazy<T[]> ILazySessionOperations.Load<T>(params ValueType[] ids)
        {
            var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return Lazily.Load<T>(documentKeys, null);
        }

        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        ILazyLoaderWithInclude<object> ILazySessionOperations.Include(string path)
        {
            return new LazyMultiLoaderWithInclude<object>(this).Include(path);
        }

        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        ILazyLoaderWithInclude<T> ILazySessionOperations.Include<T>(Expression<Func<T, object>> path)
        {
            return new LazyMultiLoaderWithInclude<T>(this).Include(path);
        }

        /// <summary>
        /// Register to lazily load documents and include
        /// </summary>
        public Lazy<T[]> LazyLoadInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes, Action<T[]> onEval)
        {
            var idsAndShards = ids.Select(id => new
            {
                id,
                shards = GetCommandsToOperateOn(new ShardRequestData
                {
                    Keys = { id },
                    EntityType = typeof(T),
                })
            })
                                  .GroupBy(x => x.shards, new DbCmdsListComparer());
            var cmds = idsAndShards.SelectMany(idAndShard => idAndShard.Key).Distinct().ToList();

            var multiLoadOperation = new MultiLoadOperation(this, () =>
            {
                var list = cmds.Select(cmd => cmd.DisableAllCaching()).ToList();
                return new DisposableAction(() => list.ForEach(disposable => disposable.Dispose()));
            }, ids, includes);
            var lazyOp = new LazyMultiLoadOperation<T>(multiLoadOperation, ids, includes);
            return AddLazyOperation(lazyOp, onEval, cmds);
        }

        public ResponseTimeInformation ExecuteAllPendingLazyOperations()
        {
            if (pendingLazyOperations.Count == 0)
                return new ResponseTimeInformation();

            try
            {
                var sw = Stopwatch.StartNew();
                IncrementRequestCount();
                var responseTimeDuration = new ResponseTimeInformation();
                while (ExecuteLazyOperationsSingleStep())
                {
                    Thread.Sleep(100);
                }
                responseTimeDuration.ComputeServerTotal();

                foreach (var pendingLazyOperation in pendingLazyOperations)
                {
                    Action<object> value;
                    if (onEvaluateLazy.TryGetValue(pendingLazyOperation.Item1, out value))
                        value(pendingLazyOperation.Item1.Result);
                }

                responseTimeDuration.TotalClientDuration = sw.Elapsed;
                return responseTimeDuration;
            }
            finally
            {
                pendingLazyOperations.Clear();
            }
        }

        private bool ExecuteLazyOperationsSingleStep()
        {
            var disposables = pendingLazyOperations.Select(x => x.Item1.EnterContext()).Where(x => x != null).ToList();
            try
            {
                var operationsPerShardGroup = pendingLazyOperations.GroupBy(x => x.Item2, new DbCmdsListComparer());

                foreach (var operationPerShard in operationsPerShardGroup)
                {
                    var lazyOperations = operationPerShard.Select(x => x.Item1).ToArray();
                    var requests = lazyOperations.Select(x => x.CreateRequest()).ToArray();
                    var multiResponses = shardStrategy.ShardAccessStrategy.Apply(operationPerShard.Key, new ShardRequestData(),
                                                                                 (commands, i) => commands.MultiGet(requests));

                    var sb = new StringBuilder();
                    foreach (var response in from shardResponses in multiResponses
                                             from getResponse in shardResponses
                                             where getResponse.RequestHasErrors()
                                             select getResponse)
                        sb.AppendFormat("Got an error from server, status code: {0}{1}{2}", response.Status, Environment.NewLine,
                                        response.Result)
                          .AppendLine();

                    if (sb.Length > 0)
                        throw new InvalidOperationException(sb.ToString());

                    for (int i = 0; i < lazyOperations.Length; i++)
                    {
                        var copy = i;
                        lazyOperations[i].HandleResponses(multiResponses.Select(x => x[copy]).ToArray(), shardStrategy);
                        if (lazyOperations[i].RequiresRetry)
                            return true;
                    }
                }
                return false;
            }
            finally
            {
                disposables.ForEach(disposable => disposable.Dispose());
            }
        }

        #endregion

        #endregion

        #region Queries

        public override RavenQueryInspector<T> CreateRavenQueryInspector<T>()
        {
            return new ShardedRavenQueryInspector<T>(shardStrategy,
                                                     shardDbCommands.Values.ToList(),
                                                     null);
        }

        protected override IDocumentQuery<T> DocumentQueryGeneratorQuery<T>(string indexName, bool isMapReduce = false)
        {
            return DocumentQuery<T>(indexName, isMapReduce);
        }

        protected override IAsyncDocumentQuery<T> DocumentQueryGeneratorAsyncQuery<T>(string indexName, bool isMapReduce = false)
        {
            throw new NotSupportedException("The synchronous sharded document store doesn't support async operations");
        }

        [Obsolete("Use DocumentQuery instead.")]
        public IDocumentQuery<T> LuceneQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
        {
            return DocumentQuery<T, TIndexCreator>();
        }

        public IDocumentQuery<T> DocumentQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexName = new TIndexCreator().IndexName;
            return DocumentQuery<T>(indexName);
        }

        [Obsolete("Use DocumentQuery instead")]
        public IDocumentQuery<T> LuceneQuery<T>(string indexName, bool isMapReduce = false)
        {
            return DocumentQuery<T>(indexName, isMapReduce);
        }

        public IDocumentQuery<T> DocumentQuery<T>(string indexName, bool isMapReduce = false)
        {
            return new ShardedDocumentQuery<T>(this, GetShardsToOperateOn, shardStrategy, indexName, null, null,
                                               theListeners.QueryListeners, isMapReduce);
        }

        [Obsolete("Use DocumentQuery instead.")]
        public IDocumentQuery<T> LuceneQuery<T>()
        {
            return DocumentQuery<T>();
        }

        public IDocumentQuery<T> DocumentQuery<T>()
        {
            return DocumentQuery<T>(GetDynamicIndexName<T>());
        }

        #endregion

        /// <summary>
        /// Saves all the changes to the Raven server.
        /// </summary>
        void IDocumentSession.SaveChanges()
        {
            using (EntityToJson.EntitiesToJsonCachingScope())
            {
                var data = PrepareForSaveChanges();
                if (data.Commands.Count == 0 && deferredCommandsByShard.Count == 0)
                    return; // nothing to do here

                IncrementRequestCount();
                LogBatch(data);

                // split by shards
                var saveChangesPerShard = GetChangesToSavePerShard(data);

                // execute on all shards
                foreach (var shardAndObjects in saveChangesPerShard)
                {
                    var shardId = shardAndObjects.Key;

                    IDatabaseCommands databaseCommands;
                    if (shardDbCommands.TryGetValue(shardId, out databaseCommands) == false)
                        throw new InvalidOperationException(
                            string.Format("ShardedDocumentStore can't find a DatabaseCommands for shard id '{0}'.", shardId));

                    var results = databaseCommands.Batch(shardAndObjects.Value.Commands, data.Options);
                    UpdateBatchResults(results, shardAndObjects.Value);
                }
            }
        }

        protected Dictionary<string, SaveChangesData> GetChangesToSavePerShard(SaveChangesData data)
        {
            var saveChangesPerShard = CreateSaveChangesBatchPerShardFromDeferredCommands();

            for (int index = 0; index < data.Entities.Count; index++)
            {
                var entity = data.Entities[index];
                var metadata = GetMetadataFor(entity);
                var shardId = metadata.Value<string>(Constants.RavenShardId);
                if (shardId == null)
                    throw new InvalidOperationException("Cannot save a document when the shard id isn't defined. Missing Raven-Shard-Id in the metadata");
                var shardSaveChangesData = saveChangesPerShard.GetOrAdd(shardId);
                shardSaveChangesData.Entities.Add(entity);
                shardSaveChangesData.Commands.Add(data.Commands[index]);
            }
            return saveChangesPerShard;
        }


        void ISyncAdvancedSessionOperation.Refresh<T>(T entity)
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

            var results = shardStrategy.ShardAccessStrategy.Apply(dbCommands, shardRequestData, (dbCmd, i) =>
            {
                var jsonDocument = dbCmd.Get(value.Key);
                if (jsonDocument == null)
                    return false;

                value.Metadata = jsonDocument.Metadata;
                value.OriginalMetadata = (RavenJObject)jsonDocument.Metadata.CloneToken();
                value.ETag = jsonDocument.Etag;
                value.OriginalValue = jsonDocument.DataAsJson;
                var newEntity = ConvertToEntity(typeof(T),value.Key, jsonDocument.DataAsJson, jsonDocument.Metadata);
                foreach (
                    var property in ReflectionUtil.GetPropertiesAndFieldsFor(entity.GetType(), BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
                    .Where(property => property.CanWrite() && property.CanRead() && property.GetIndexParameters().Length == 0))
                {
                    property.SetValue(entity, property.GetValue(newEntity));
                }
                return true;
            });

            if (results.All(x => x == false))
            {
                throw new InvalidOperationException("Document '" + value.Key + "' no longer exists and was probably deleted");
            }
        }

        public T[] LoadStartingWith<T>(string keyPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null, RavenPagingInformation pagingInformation = null, string skipAfter = null)
        {
            IncrementRequestCount();
            var shards = GetCommandsToOperateOn(new ShardRequestData
            {
                EntityType = typeof(T),
                Keys = { keyPrefix }
            });
            var results = shardStrategy.ShardAccessStrategy.Apply(shards, new ShardRequestData
            {
                EntityType = typeof(T),
                Keys = { keyPrefix }
            }, (dbCmd, i) => dbCmd.StartsWith(keyPrefix, matches, start, pageSize, exclude: exclude, skipAfter: skipAfter));

            return results.SelectMany(x => x).Select(TrackEntity<T>)
                          .ToArray();
        }

        public TResult[] LoadStartingWith<TTransformer, TResult>(string keyPrefix, string matches = null, int start = 0,
                                                                 int pageSize = 25, string exclude = null,
                                                                 RavenPagingInformation pagingInformation = null,
                                                                 Action<ILoadConfiguration> configure = null, 
                                                                 string skipAfter = null) where TTransformer : AbstractTransformerCreationTask, new()
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

            var results = shardStrategy.ShardAccessStrategy.Apply(shards, new ShardRequestData
            {
                EntityType = typeof (TResult),
                Keys = {keyPrefix}
            },
            (dbCmd, i) =>
            dbCmd.StartsWith(keyPrefix, matches, start, pageSize,
                            exclude: exclude, transformer: transformer,
                            transformerParameters: configuration.TransformerParameters,
                            skipAfter: skipAfter));
            var queryOperation = new QueryOperation(this, "Load/StartingWith", null, null, false, TimeSpan.Zero, null, null, false);

            return results.SelectMany(x => x).Select(x=>queryOperation.Deserialize<TResult>(x.ToJson()))
                          .ToArray();
        }

        public Lazy<TResult[]> MoreLikeThis<TResult>(MoreLikeThisQuery query)
        {
            throw new NotSupportedException("Not supported for sharded session");
        }

        Lazy<T[]> ILazySessionOperations.LoadStartingWith<T>(string keyPrefix, string matches, int start, int pageSize, string exclude, RavenPagingInformation pagingInformation, string skipAfter)
        {
            IncrementRequestCount();
            var cmds = GetCommandsToOperateOn(new ShardRequestData
            {
                EntityType = typeof(T),
                Keys = { keyPrefix }
            });

            var lazyLoadOperation = new LazyStartsWithOperation<T>(keyPrefix, matches, exclude, start, pageSize, this, null, skipAfter);

            return AddLazyOperation<T[]>(lazyLoadOperation, null, cmds);
        }

        public IAsyncDatabaseCommands AsyncDatabaseCommands
        {
            get { throw new NotSupportedException("Not supported for sharded session"); }
        }

        string ISyncAdvancedSessionOperation.GetDocumentUrl(object entity)
        {
            DocumentMetadata value;
            if (entitiesAndMetadata.TryGetValue(entity, out value) == false)
                throw new ArgumentException("The entity is not part of the session");

            var shardId = value.Metadata.Value<string>(Constants.RavenShardId);
            IDatabaseCommands commands;
            if (shardDbCommands.TryGetValue(shardId, out commands) == false)
                throw new InvalidOperationException("Could not find matching shard for shard id: " + shardId);
            return commands.UrlFor(value.Key);
        }

        public IEnumerator<StreamResult<T>> Stream<T>(IQueryable<T> query)
        {
            QueryHeaderInformation _;
            return Stream(query, out _);
        }

        public IEnumerator<StreamResult<T>> Stream<T>(IQueryable<T> query, out QueryHeaderInformation queryHeaderInformation)
        {
            throw new NotSupportedException("Streams are currently not supported by sharded document store");
        }

        public IEnumerator<StreamResult<T>> Stream<T>(IDocumentQuery<T> query)
        {
            QueryHeaderInformation _;
            return Stream(query, out _);
        }

        public IEnumerator<StreamResult<T>> Stream<T>(IDocumentQuery<T> query, out QueryHeaderInformation queryHeaderInformation)
        {
            throw new NotSupportedException("Streams are currently not supported by sharded document store");
        }

        public IEnumerator<StreamResult<T>> Stream<T>(Etag fromEtag, int start = 0, int pageSize = Int32.MaxValue, RavenPagingInformation pagingInformation = null, string transformer = null, Dictionary<string, RavenJToken> transformerParameters = null)
        {
            throw new NotSupportedException("Streams are currently not supported by sharded document store");
        }

        public IEnumerator<StreamResult<T>> Stream<T>(string startsWith, string matches = null, int start = 0, int pageSize = Int32.MaxValue, RavenPagingInformation pagingInformation = null, string skipAfter = null, string transformer = null, Dictionary<string, RavenJToken> transformerParameters = null)
        {
            throw new NotSupportedException("Streams are currently not supported by sharded document store");
        }

        public Operation DeleteByIndex<T, TIndexCreator>(Expression<Func<T, bool>> expression) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return DeleteByIndex<T>(indexCreator.IndexName, expression);
        }

        public Operation DeleteByIndex<T>(string indexName, Expression<Func<T, bool>> expression)
        {
            var query = Query<T>(indexName).Where(expression);
            var indexQuery = new IndexQuery()
            {
                Query = query.ToString()
            };

            var shards = GetCommandsToOperateOn(new ShardRequestData
            {
                EntityType = typeof(T),
                Keys = { indexName }
            });
            var operations = shardStrategy.ShardAccessStrategy.Apply(shards, new ShardRequestData
            {
                EntityType = typeof(T),
                Keys = { indexName }
            }, (dbCmd, i) => dbCmd.DeleteByIndex(indexName, indexQuery));

            var shardOperation = new ShardsOperation(operations);

            return shardOperation;
        }

        public FacetResults[] MultiFacetedSearch(params FacetQuery[] queries)
        {
            throw new NotSupportedException("Multi faceted searching is currently not supported by sharded document store");
        }
    }
}
