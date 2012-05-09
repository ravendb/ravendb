//-----------------------------------------------------------------------
// <copyright file="ShardedDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !NET35
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Connection.Async;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using System.Collections.Concurrent;

namespace Raven.Client.Shard
{
	/// <summary>
	/// Implements Unit of Work for accessing a set of sharded RavenDB servers
	/// </summary>
	public class AsyncShardedDocumentSession : BaseShardedDocumentSession<IAsyncDatabaseCommands>,
		IAsyncDocumentSessionImpl, IAsyncAdvancedSessionOperations
	{
		private ConcurrentQueue<object> entitiesStoredWithoutIDs = new ConcurrentQueue<object>();

		public AsyncShardedDocumentSession(ShardedDocumentStore documentStore, DocumentSessionListeners listeners, Guid id,
			ShardStrategy shardStrategy, IDictionary<string, IAsyncDatabaseCommands> shardDbCommands)
			: base(documentStore, listeners, id, shardStrategy, shardDbCommands)
		{
			GenerateDocumentKeysOnStore = false;
		}

		protected override JsonDocument GetJsonDocument(string documentKey)
		{
			throw new NotSupportedException("This method requires a syncronous call to the server, which is not supported by the async session");
		}

		#region Properties to access different interfacess

		IAsyncAdvancedSessionOperations IAsyncDocumentSession.Advanced
		{
			get { return this; }
		}

		#endregion

		#region Load and Include

		public Task<T> LoadAsync<T>(string id)
		{
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
					return commands.GetAsync(id).ContinueWith(task =>
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
				});
			});

			return results.ContinueWith(task =>
			{
				var shardsContainThisDocument = task.Result.Where(x => !Equals(x, default(T))).ToArray();
				if (shardsContainThisDocument.Count() > 1)
				{
					throw new InvalidOperationException("Found document with id: " + id + " on more than a single shard, which is not allowed. Document keys have to be unique cluster-wide.");
				}

				return shardsContainThisDocument.FirstOrDefault();
			});
		}

		public Task<T> LoadAsync<T>(ValueType id)
		{
			var documentKey = Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false);
			return LoadAsync<T>(documentKey);
		}

		public Task<T[]> LoadAsync<T>(string[] ids)
		{
			return LoadAsyncInternal<T>(ids);
		}

		public Task<T[]> LoadAsyncInternal<T>(string[] ids)
		{
			return LoadAsyncInternal<T>(ids, null);
		}

		public Task<T[]> LoadAsyncInternal<T>(string[] ids, string[] includes)
		{
			var results = new T[ids.Length];
			var idsToLoad = GetIdsThatNeedLoading<T>(ids, includes);

			if (!idsToLoad.Any())
				return CompletedTask.With(new T[ids.Length]);

			IncrementRequestCount();

			var loadTasks = idsToLoad.Select(shardsAndIds => (Func<Task>)(() =>
			{
				var shards = shardsAndIds.Key;
				var idsForCurrentShards = shardsAndIds.Select(x => x.Id).ToArray();

				var multiLoadOperations = shardStrategy.ShardAccessStrategy.ApplyAsync(shards, new ShardRequestData
				{
					EntityType = typeof(T),
					Keys = idsForCurrentShards
				}, (commands, i) =>
				{
					var multiLoadOperation = new MultiLoadOperation(this, commands.DisableAllCaching, idsForCurrentShards);

					Func<Task<MultiLoadOperation>> executer = null;
					executer = () =>
					{
						multiLoadOperation.LogOperation();

						IDisposable loadContext = multiLoadOperation.EnterMultiLoadContext();
						return commands.GetAsync(idsForCurrentShards, includes).ContinueWith(task =>
						{
							loadContext.Dispose();

							if (multiLoadOperation.SetResult(task.Result))
								return executer();
							return CompletedTask.With(multiLoadOperation);
						}).Unwrap();
					};

					return executer();
				});

				return multiLoadOperations.ContinueWith(task =>
				{
					foreach (var loadResults in task.Result.Select(multiLoadOperation => multiLoadOperation.Complete<T>()))
					{
						for (int i = 0; i < loadResults.Length; i++)
						{
							if (ReferenceEquals(loadResults[i], null))
								continue;
							var id = idsForCurrentShards[i];
							var itemPosition = Array.IndexOf(ids, id);
							if (ReferenceEquals(results[itemPosition], default(T)) == false)
							{
								throw new InvalidOperationException("Found document with id: " + id + " on more than a single shard, which is not allowed. Document keys have to be unique cluster-wide.");
							}
							results[itemPosition] = loadResults[i];
						}
					}
				});
			}));

			return loadTasks.StartInParallel().WithResult(() => results);
		}

		public IAsyncLoaderWithInclude<object> Include(string path)
		{
			return new AsyncMultiLoaderWithInclude<object>(this).Include(path);
		}

		public IAsyncLoaderWithInclude<T> Include<T>(Expression<Func<T, object>> path)
		{
			return new AsyncMultiLoaderWithInclude<T>(this).Include(path);
		}

		#endregion

		#region Queries

		protected override IDocumentQuery<T> IDocumentQueryGeneratorQuery<T>(string indexName)
		{
			throw new NotSupportedException("The async sharded document store doesn't support synchronous operations");
		}

		protected override IAsyncDocumentQuery<T> IDocumentQueryGeneratorAsyncQuery<T>(string indexName)
		{
			return AsyncLuceneQuery<T>(indexName);
		}

		public IAsyncDocumentQuery<T> AsyncLuceneQuery<T>(string indexName)
		{
			return new AsyncShardedDocumentQuery<T>(this, GetShardsToOperateOn, shardStrategy, indexName, null, listeners.QueryListeners);
		}

		public IAsyncDocumentQuery<T> AsyncLuceneQuery<T>()
		{
			return AsyncLuceneQuery<T>(GetDynamicIndexName<T>());
		}

		#endregion

		#region DatabaseCommands (not supported)

		Raven.Client.Connection.Async.IAsyncDatabaseCommands IAsyncAdvancedSessionOperations.AsyncDatabaseCommands
		{
			get { throw new NotSupportedException("Not supported in a sharded session"); }
		}

		#endregion

		/// <summary>
		/// Saves all the changes to the Raven server.
		/// </summary>
		Task IAsyncDocumentSession.SaveChangesAsync()
		{
			return GenerateDocumentKeysForSaveChanges().ContinueWith(keysTask =>
				{
					keysTask.AssertNotFailed();

					var cachingScope = EntitiesToJsonCachingScope();
					try
					{
						var data = PrepareForSaveChanges();
						if (data.Commands.Count == 0 && deferredCommandsByShard.Count == 0)
							return new CompletedTask(); // nothing to do here

						IncrementRequestCount();
						LogBatch(data);

						// split by shards
						var saveChangesPerShard = GetChangesToSavePerShard(data);

						var saveTasks = new List<Func<Task<BatchResult[]>>>();
						var saveChanges = new List<SaveChangesData>();
						// execute on all shards
						foreach (var shardAndObjects in saveChangesPerShard)
						{
							var shardId = shardAndObjects.Key;

							IAsyncDatabaseCommands databaseCommands;
							if (shardDbCommands.TryGetValue(shardId, out databaseCommands) == false)
								throw new InvalidOperationException(string.Format("ShardedDocumentStore cannot found a DatabaseCommands for shard id '{0}'.", shardId));

							saveChanges.Add(shardAndObjects.Value);
							saveTasks.Add(() => databaseCommands.BatchAsync(shardAndObjects.Value.Commands.ToArray()));
						}

						return saveTasks.StartInParallel().ContinueWith(task =>
						{
							try
							{
								var results = task.Result;
								for (int index = 0; index < results.Length; index++)
								{
									UpdateBatchResults(results[index], saveChanges[index]);
								}
							}
							finally
							{
								cachingScope.Dispose();
							}
						});
					}
					catch
					{
						cachingScope.Dispose();
						throw;
					}
				}).Unwrap();
		}

		private Task GenerateDocumentKeysForSaveChanges()
		{
			object entity;
			if (entitiesStoredWithoutIDs.TryDequeue(out entity))
			{
				DocumentMetadata metadata;
				if (entitiesAndMetadata.TryGetValue(entity, out metadata))
				{
					return GenerateDocumentKeyForStorageAsync(entity)
						.ContinueWith(task => metadata.Key = ModifyObjectId(task.Result, entity, metadata.Metadata))
						.ContinueWithTask(GenerateDocumentKeysForSaveChanges);
				}
			}
		
			return new CompletedTask();
		}

		protected override void RememberEntityForDocumentKeyGeneration(object entity)
		{
			entitiesStoredWithoutIDs.Enqueue(entity);
		}
	}
}
#endif
