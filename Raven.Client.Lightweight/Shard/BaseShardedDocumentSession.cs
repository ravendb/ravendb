using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Document;
using Raven.Client.Document.Batches;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Client.Util;
using Raven.Json.Linq;

#if !NET35
namespace Raven.Client.Shard
{
	public abstract class BaseShardedDocumentSession<TDatabaseCommands> : InMemoryDocumentSessionOperations, IDocumentQueryGenerator, ITransactionalDocumentSession
		where TDatabaseCommands : class
	{
#if !NET35
		protected readonly List<Tuple<ILazyOperation, IList<TDatabaseCommands>>> pendingLazyOperations = new List<Tuple<ILazyOperation, IList<TDatabaseCommands>>>();
		protected readonly Dictionary<ILazyOperation, Action<object>> onEvaluateLazy = new Dictionary<ILazyOperation, Action<object>>();
#endif
		protected readonly IDictionary<string, List<ICommandData>> deferredCommandsByShard = new Dictionary<string, List<ICommandData>>();
		protected readonly ShardStrategy shardStrategy;
		protected readonly IDictionary<string, TDatabaseCommands> shardDbCommands;

		public BaseShardedDocumentSession(ShardedDocumentStore documentStore, DocumentSessionListeners listeners, Guid id,
			ShardStrategy shardStrategy, IDictionary<string, TDatabaseCommands> shardDbCommands)
			: base(documentStore, listeners, id)
		{
			this.shardStrategy = shardStrategy;
			this.shardDbCommands = shardDbCommands;
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

		protected IEnumerable<IGrouping<IList<TDatabaseCommands>, IdToLoad<T>>> GetIdsThatNeedLoading<T>(string[] ids, string[] includes)
		{
			string[] idsToLoad;
			if (includes != null)
			{
				// Need to load everything, for the includes
				idsToLoad = ids;
			}
			else
			{
				// Only load items which aren't already loaded
				idsToLoad = ids.Where(id => IsLoaded(id) == false)
					.Distinct(StringComparer.InvariantCultureIgnoreCase)
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
			string indexName = "dynamic";
			if (typeof(T).IsEntityType())
			{
				indexName += "/" + Conventions.GetTypeTagName(typeof(T));
			}
			return indexName;
		}

		protected Dictionary<string, SaveChangesData> GetChangesToSavePerShard(SaveChangesData data)
		{
			var saveChangesPerShard = new Dictionary<string, SaveChangesData>();

			foreach (var deferredCommands in deferredCommandsByShard)
			{
				var saveChangesData = saveChangesPerShard.GetOrAdd(deferredCommands.Key);
				saveChangesData.DeferredCommandsCount += deferredCommands.Value.Count;
				saveChangesData.Commands.AddRange(deferredCommands.Value);
			}
			deferredCommandsByShard.Clear();

			for (int index = 0; index < data.Entities.Count; index++)
			{
				var entity = data.Entities[index];
				var metadata = GetMetadataFor(entity);
				var shardId = metadata.Value<string>(Constants.RavenShardId);

				var shardSaveChangesData = saveChangesPerShard.GetOrAdd(shardId);
				shardSaveChangesData.Entities.Add(entity);
				shardSaveChangesData.Commands.Add(data.Commands[index]);
			}
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

		protected override void StoreEntityInUnitOfWork(string id, object entity, Guid? etag, RavenJObject metadata, bool forceConcurrencyCheck)
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
			var shardId = shardStrategy.ShardResolutionStrategy.GenerateShardIdFor(entity);
			if (string.IsNullOrEmpty(shardId))
				throw new InvalidOperationException("Could not find shard id for " + entity + " because " + shardStrategy.ShardAccessStrategy + " returned null or empty string for the document shard id.");
			metadata[Constants.RavenShardId] = shardId;
			var modifyDocumentId = shardStrategy.ModifyDocumentId(Conventions, shardId, id);
			if (modifyDocumentId != id)
				TrySetIdentity(entity, modifyDocumentId);

			return modifyDocumentId;
		}

		#endregion

		#region Transaction methods (not supported)

		public override void Commit(Guid txId)
		{
			throw new NotSupportedException("DTC support is handled via the internal document stores");
		}

		public override void Rollback(Guid txId)
		{
			throw new NotSupportedException("DTC support is handled via the internal document stores");
		}

		/// <summary>
		/// Promotes a transaction specified to a distributed transaction
		/// </summary>
		/// <param name="fromTxId">From tx id.</param>
		/// <returns>The token representing the distributed transaction</returns>
		public override byte[] PromoteTransaction(Guid fromTxId)
		{
			throw new NotSupportedException("DTC support is handled via the internal document stores");
		}

		/// <summary>
		/// Stores the recovery information for the specified transaction
		/// </summary>
		/// <param name="resourceManagerId">The resource manager Id for this transaction</param>
		/// <param name="txId">The tx id.</param>
		/// <param name="recoveryInformation">The recovery information.</param>
		public void StoreRecoveryInformation(Guid resourceManagerId, Guid txId, byte[] recoveryInformation)
		{
			throw new NotSupportedException("DTC support is handled via the internal document stores");
		}

		protected override void TryEnlistInAmbientTransaction()
		{
			// we DON'T support enlisting at the sharded document store level, only at the managed document stores, which 
			// turns out to be pretty much the same thing
		}

		#endregion

		#region Queries

		/// <summary>
		/// Queries the specified index using Linq.
		/// </summary>
		/// <typeparam name="T">The result of the query</typeparam>
		/// <param name="indexName">Name of the index.</param>
		/// <returns></returns>
		public IRavenQueryable<T> Query<T>(string indexName)
		{
			var ravenQueryStatistics = new RavenQueryStatistics();
			var provider = new RavenQueryProvider<T>(this, indexName, ravenQueryStatistics, null
#if !NET35
, null
#endif
);
			return new RavenQueryInspector<T>(provider, ravenQueryStatistics, indexName, null, this, null
#if !NET35
, null
#endif
);
		}

		/// <summary>
		/// Query RavenDB dynamically using LINQ
		/// </summary>
		/// <typeparam name="T">The result of the query</typeparam>
		public IRavenQueryable<T> Query<T>()
		{
			var indexName = "dynamic";
			if (typeof(T).IsEntityType())
			{
				indexName += "/" + Conventions.GetTypeTagName(typeof(T));
			}
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
			return Query<T>(indexCreator.IndexName)
				.Customize(x => x.TransformResults(indexCreator.ApplyReduceFunctionIfExists));
		}

		/// <summary>
		/// Implements IDocumentQueryGenerator.Query
		/// </summary>
		protected abstract IDocumentQuery<T> IDocumentQueryGeneratorQuery<T>(string indexName);

		IDocumentQuery<T> IDocumentQueryGenerator.Query<T>(string indexName)
		{
			return IDocumentQueryGeneratorQuery<T>(indexName);
		}

#if !NET35
		/// <summary>
		/// Implements IDocumentQueryGenerator.AsyncQuery
		/// </summary>
		protected abstract IAsyncDocumentQuery<T> IDocumentQueryGeneratorAsyncQuery<T>(string indexName);

		IAsyncDocumentQuery<T> IDocumentQueryGenerator.AsyncQuery<T>(string indexName)
		{
			return IDocumentQueryGeneratorAsyncQuery<T>(indexName);
		}
#endif

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
	}
}
#endif