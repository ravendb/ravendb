using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Document.Batches;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Json.Linq;

namespace Raven.Client.Shard
{
    public abstract class BaseShardedDocumentSession<TDatabaseCommands> : InMemoryDocumentSessionOperations, IDocumentQueryGenerator
        where TDatabaseCommands : class
    {
        protected new readonly List<Tuple<ILazyOperation, IList<TDatabaseCommands>>> pendingLazyOperations = new List<Tuple<ILazyOperation, IList<TDatabaseCommands>>>();
        protected new readonly Dictionary<ILazyOperation, Action<object>> onEvaluateLazy = new Dictionary<ILazyOperation, Action<object>>();
        protected readonly IDictionary<string, List<ICommandData>> deferredCommandsByShard = new Dictionary<string, List<ICommandData>>();
        public readonly ShardStrategy shardStrategy;
        protected readonly IDictionary<string, TDatabaseCommands> shardDbCommands;


        protected BaseShardedDocumentSession(string dbName, ShardedDocumentStore documentStore, DocumentSessionListeners listeners, Guid id,
            ShardStrategy shardStrategy, IDictionary<string, TDatabaseCommands> shardDbCommands)
            : base(dbName, documentStore, listeners, id)
        {
            this.shardStrategy = shardStrategy;
            this.shardDbCommands = shardDbCommands;
        }

        public override string DatabaseName
        {
            get { return _databaseName; }
        }

        #region Sharding support methods

        protected IList<Tuple<string, TDatabaseCommands>> GetShardsToOperateOn(ShardRequestData resultionData)
        {
            var shardIds = shardStrategy.ShardResolutionStrategy.PotentialShardsFor(resultionData);

            IEnumerable<KeyValuePair<string, TDatabaseCommands>> cmds = shardDbCommands;

            if (shardIds == null)
            {
                return cmds.Select(x => Tuple.Create(x.Key, x.Value)).ToList();
            }

            var list = new List<Tuple<string, TDatabaseCommands>>();
            foreach (var shardId in shardIds)
            {
                TDatabaseCommands value;
                if (shardDbCommands.TryGetValue(shardId, out value) == false)
                    throw new InvalidOperationException("Could not find shard id: " + shardId);

                list.Add(Tuple.Create(shardId, value));

            }
            return list;
        }

        protected IList<TDatabaseCommands> GetCommandsToOperateOn(ShardRequestData resultionData)
        {
            return GetShardsToOperateOn(resultionData).Select(x => x.Item2).ToList();
        }

        protected IEnumerable<IGrouping<IList<TDatabaseCommands>, IdToLoad<T>>> GetIdsThatNeedLoading<T>(string[] ids, string[] includes, string transformer)
        {
            string[] idsToLoad;
            if (includes != null || string.IsNullOrEmpty(transformer) == false)
            {
                // Need to load everything, for the includes
                idsToLoad = ids;
            }
            else
            {
                // Only load items which aren't already loaded
                idsToLoad = ids.Where(id => IsLoaded(id) == false)
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToArray();
            }

            var idsAndShards = idsToLoad.Select(id => new IdToLoad<T>(
                id,
                GetCommandsToOperateOn(new ShardRequestData
                {
                    EntityType = typeof(T),
                    Keys = { id }
                })
            )).GroupBy(x => x.Shards, new DbCmdsListComparer());

            return idsAndShards;
        }

        protected string GetDynamicIndexName<T>()
        {
            string indexName = CreateDynamicIndexName<T>();
            
            return indexName;
        }

        protected Dictionary<string, SaveChangesData> CreateSaveChangesBatchPerShardFromDeferredCommands()
        {
            var saveChangesPerShard = new Dictionary<string, SaveChangesData>();

            foreach (var deferredCommands in deferredCommandsByShard)
            {
                var saveChangesData = saveChangesPerShard.GetOrAdd(deferredCommands.Key);
                saveChangesData.DeferredCommandsCount += deferredCommands.Value.Count;
                saveChangesData.Commands.AddRange(deferredCommands.Value);
            }
            deferredCommandsByShard.Clear();
            return saveChangesPerShard;
        }

        #endregion

       #region InMemoryDocumentSessionOperations implementation

        public override void Defer(params ICommandData[] commands)
        {
            var cmdsByShard = commands.Select(cmd =>
            {
                var shardsToOperateOn = GetShardsToOperateOn(new ShardRequestData
                {
                    Keys = { cmd.Key }
                }).Select(x => x.Item1).ToList();

                if (shardsToOperateOn.Count == 0)
                {
                    throw new InvalidOperationException("Cannot execute " + cmd.Method + " on " + cmd.Key +
                                                        " because it matched no shards");
                }

                if (shardsToOperateOn.Count > 1)
                {
                    throw new InvalidOperationException("Cannot execute " + cmd.Method + " on " + cmd.Key +
                                                        " because it matched multiple shards");

                }

                return new
                {
                    shard = shardsToOperateOn[0],
                    cmd
                };
            }).GroupBy(x => x.shard);

            foreach (var cmdByShard in cmdsByShard)
            {
                deferredCommandsByShard.GetOrAdd(cmdByShard.Key).AddRange(cmdByShard.Select(x => x.cmd));
            }
        }

        protected override void StoreEntityInUnitOfWork(string id, object entity, long? etag, RavenJObject metadata, bool forceConcurrencyCheck)
        {
            string modifyDocumentId = null;
            if (id != null)
            {
                modifyDocumentId = ModifyObjectId(id, entity, metadata);
            }
            base.StoreEntityInUnitOfWork(modifyDocumentId, entity, etag, metadata, forceConcurrencyCheck);
        }

        protected string ModifyObjectId(string id, object entity, RavenJObject metadata)
        {
            var shardId = shardStrategy.ShardResolutionStrategy.GenerateShardIdFor(entity, this);
            if (string.IsNullOrEmpty(shardId))
                throw new InvalidOperationException("Could not find shard id for " + entity + " because " + shardStrategy.ShardAccessStrategy + " returned null or empty string for the document shard id.");
            metadata[Constants.Headers.RavenShardId] = shardId;
            var modifyDocumentId = shardStrategy.ModifyDocumentId(Conventions, shardId, id);
            if (modifyDocumentId != id)
                GenerateEntityIdOnTheClient.TrySetIdentity(entity, modifyDocumentId);

            return modifyDocumentId;
        }

        #endregion

        #region Queries

        /// <summary>
        /// Queries the specified index using Linq.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <param name="indexName">Name of the index.</param>
        /// <param name="isMapReduce">Whatever we are querying a map/reduce index (modify how we treat identifier properties)</param>
        public IRavenQueryable<T> Query<T>(string indexName, bool isMapReduce = false)
        {
            var ravenQueryStatistics = new RavenQueryStatistics();
            var highlightings = new RavenQueryHighlightings();
            var provider = new RavenQueryProvider<T>(this, indexName, ravenQueryStatistics, highlightings, null, null, isMapReduce);
            var ravenQueryInspector = CreateRavenQueryInspector<T>();
            ravenQueryInspector.Init(provider, ravenQueryStatistics, highlightings, indexName, null, this, null, null, isMapReduce);
            return ravenQueryInspector;
        }

        public abstract RavenQueryInspector<T> CreateRavenQueryInspector<T>();

        /// <summary>
        /// Query RavenDB dynamically using LINQ
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        public IRavenQueryable<T> Query<T>()
        {
            var indexName = CreateDynamicIndexName<T>();
            
            return Query<T>(indexName)
                .Customize(x => x.TransformResults((query, results) => results.Take(query.PageSize)));
        }

        /// <summary>
        /// Queries the index specified by <typeparamref name="TIndexCreator"/> using Linq.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
        /// <returns></returns>
        public IRavenQueryable<T> Query<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator
            {
                Conventions = Conventions
            };
            return Query<T>(indexCreator.IndexName, indexCreator.IsMapReduce)
                .Customize(x => x.TransformResults(indexCreator.ApplyReduceFunctionIfExists));
        }

        /// <summary>
        /// Implements IDocumentQueryGenerator.Query
        /// </summary>
        protected abstract IDocumentQuery<T> DocumentQueryGeneratorQuery<T>(string indexName, bool isMapReduce);

        IDocumentQuery<T> IDocumentQueryGenerator.Query<T>(string indexName, bool isMapReduce)
        {
            return DocumentQueryGeneratorQuery<T>(indexName, isMapReduce);
        }

        /// <summary>
        /// Implements IDocumentQueryGenerator.AsyncQuery
        /// </summary>
        protected abstract IAsyncDocumentQuery<T> DocumentQueryGeneratorAsyncQuery<T>(string indexName, bool isMapReduce);

        IAsyncDocumentQuery<T> IDocumentQueryGenerator.AsyncQuery<T>(string indexName, bool isMapReduce)
        {
            return DocumentQueryGeneratorAsyncQuery<T>(indexName, isMapReduce);
        }

        #endregion

        internal class DbCmdsListComparer : IEqualityComparer<IList<TDatabaseCommands>>
        {
            public bool Equals(IList<TDatabaseCommands> x, IList<TDatabaseCommands> y)
            {
                if (x.Count != y.Count)
                    return false;

                return !x.Where((t, i) => t != y[i]).Any();
            }

            public int GetHashCode(IList<TDatabaseCommands> obj)
            {
                return obj.Aggregate(obj.Count, (current, item) => (current * 397) ^ item.GetHashCode());
            }
        }

        protected struct IdToLoad<T>
        {
            public IdToLoad(string id, IList<TDatabaseCommands> shards)
            {
                this.Id = id;
                this.Shards = shards;
            }

            public readonly string Id;
            public readonly IList<TDatabaseCommands> Shards;
        }

        internal class ShardsOperation : Operation
        {
            private Operation[] shardsOperations;
            internal ShardsOperation(Operation[] shardsOperations) : base(-1, null)
            {
                this.shardsOperations = shardsOperations;
            }

            public override RavenJToken WaitForCompletion()
            {
                RavenJToken rc = null;
                foreach (var op in shardsOperations)
                    rc = op.WaitForCompletion();
                return rc;
            }

            public override async Task<RavenJToken> WaitForCompletionAsync()
            {
                RavenJToken rc = null;
                foreach (var op in shardsOperations)
                {
                    rc = await op.WaitForCompletionAsync().ConfigureAwait(false);
                }
                return rc;
            }
        }
    }
}
