//-----------------------------------------------------------------------
// <copyright file="ShardedDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !SILVERLIGHT
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Document;
using Raven.Client.Document.Async;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Connection.Async;
using System.Threading.Tasks;
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
					throw new InvalidOperationException("Found document with id: " + id +
														" on more than a single shard, which is not allowed. Document keys have to be unique cluster-wide.");
				}

				return shardsContainThisDocument.FirstOrDefault();
			});
		}

		public Task<T[]> LoadAsync<T>(params string[] ids)
		{
			return LoadAsyncInternal<T>(ids);
		}

		public Task<T[]> LoadAsync<T>(IEnumerable<string> ids)
		{
			return LoadAsyncInternal<T>(ids.ToArray());
		}

		public Task<T> LoadAsync<T>(ValueType id)
		{
			var documentKey = Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false);
			return LoadAsync<T>(documentKey);
		}

		public Task<T[]> LoadAsync<T>(params ValueType[] ids)
		{
			var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
			return LoadAsyncInternal<T>(documentKeys.ToArray());
		}

		public Task<T[]> LoadAsync<T>(IEnumerable<ValueType> ids)
		{
			var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
			return LoadAsyncInternal<T>(documentKeys.ToArray());
		}

		public Task<T[]> LoadAsyncInternal<T>(string[] ids)
		{
			return LoadAsyncInternal<T>(ids, null);
		}

		public async Task<T> LoadAsync<TTransformer, T>(string id) where TTransformer : AbstractTransformerCreationTask, new()
		{
            var result = await LoadAsyncInternal<T>(new[] { id }, null, new TTransformer().TransformerName).ConfigureAwait(false);
			return result.FirstOrDefault();
		}

		public async Task<T> LoadAsync<TTransformer, T>(string id, Action<ILoadConfiguration> configure) where TTransformer : AbstractTransformerCreationTask, new()
		{
			var ravenLoadConfiguration = new RavenLoadConfiguration();
			configure(ravenLoadConfiguration);
            var result = await LoadAsyncInternal<T>(new[] { id }, null, new TTransformer().TransformerName, ravenLoadConfiguration.QueryInputs).ConfigureAwait(false);
			return result.FirstOrDefault();
		}

		public Task<T[]> LoadAsync<TTransformer, T>(params string[] ids) where TTransformer : AbstractTransformerCreationTask, new()
		{
			return LoadAsyncInternal<T>(ids, null, new TTransformer().TransformerName);
		}

		public Task<TResult[]> LoadAsync<TTransformer, TResult>(IEnumerable<string> ids, Action<ILoadConfiguration> configure) where TTransformer : AbstractTransformerCreationTask, new()
		{
			var ravenLoadConfiguration = new RavenLoadConfiguration();
			configure(ravenLoadConfiguration);
			return LoadAsyncInternal<TResult>(ids.ToArray(), null, new TTransformer().TransformerName, ravenLoadConfiguration.QueryInputs);
		}

		public async Task<T[]> LoadAsyncInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes)
		{
			var results = new T[ids.Length];
			var includePaths = includes != null ? includes.Select(x => x.Key).ToArray() : null;
			var idsToLoad = GetIdsThatNeedLoading<T>(ids, includePaths).ToList();

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
                            multiLoadResult = await dbCmd.GetAsync(currentShardIds, includePaths).ConfigureAwait(false);
						}
					} while (multiLoadOperation.SetResult(multiLoadResult));
					return multiLoadOperation;
                }).ConfigureAwait(false);
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

		public async Task<T[]> LoadAsyncInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes, string transformer, Dictionary<string, RavenJToken> queryInputs = null)
		{
			var results = new T[ids.Length];
			var includePaths = includes != null ? includes.Select(x => x.Key).ToArray() : null;
			var idsToLoad = GetIdsThatNeedLoading<T>(ids, includePaths).ToList();

			if (!idsToLoad.Any())
				return results;

			IncrementRequestCount();

			if (typeof(T).IsArray)
			{
				foreach (var shard in idsToLoad)
				{
					var currentShardIds = shard.Select(x => x.Id).ToArray();
					var shardResults = await shardStrategy.ShardAccessStrategy.ApplyAsync(shard.Key,
							new ShardRequestData { EntityType = typeof(T), Keys = currentShardIds.ToList() },
							async (dbCmd, i) =>
							{
								// Returns array of arrays, public APIs don't surface that yet though as we only support Transform
								// With a single Id
                                var arrayOfArrays = (await dbCmd.GetAsync(currentShardIds, includePaths, transformer, queryInputs).ConfigureAwait(false))
															.Results
															.Select(x => x.Value<RavenJArray>("$values").Cast<RavenJObject>())
															.Select(values =>
															{
																var array = values.Select(y =>
																{
																	HandleInternalMetadata(y);
																	return ConvertToEntity<T>(null, y, new RavenJObject());
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
				var shardResults = await shardStrategy.ShardAccessStrategy.ApplyAsync(shard.Key,
						new ShardRequestData { EntityType = typeof(T), Keys = currentShardIds.ToList() },
						async (dbCmd, i) =>
						{
                            var items = (await dbCmd.GetAsync(currentShardIds, includePaths, transformer, queryInputs).ConfigureAwait(false))
								.Results
								.SelectMany(x => x.Value<RavenJArray>("$values").ToArray())
								.Select(JsonExtensions.ToJObject)
								.Select(
										x =>
										{
											HandleInternalMetadata(x);
											return ConvertToEntity<T>(null, x, new RavenJObject());
										})
								.Cast<T>()
								.ToArray();

							if (items.Length > currentShardIds.Length)
							{
								throw new InvalidOperationException(
									String.Format(
										"A load was attempted with transformer {0}, and more than one item was returned per entity - please use {1}[] as the projection type instead of {1}",
										transformer,
										typeof(T).Name));
							}

							return items;
                        }).ConfigureAwait(false);

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

		public IAsyncLoaderWithInclude<object> Include(string path)
		{
			return new AsyncMultiLoaderWithInclude<object>(this).Include(path);
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

		protected override RavenQueryInspector<T> CreateRavenQueryInspector<T>(string indexName, bool isMapReduce, RavenQueryProvider<T> provider,
																			RavenQueryStatistics ravenQueryStatistics,
																			RavenQueryHighlightings highlightings)
		{
#if !SILVERLIGHT
			return new ShardedRavenQueryInspector<T>(provider, ravenQueryStatistics, highlightings, indexName, null, this, isMapReduce, shardStrategy,
				 null,
				 shardDbCommands.Values.ToList());
#else
			return new RavenQueryInspector<T>(provider, ravenQueryStatistics, highlightings, indexName, null, this, null, isMapReduce);
#endif
		}

		protected override IDocumentQuery<T> IDocumentQueryGeneratorQuery<T>(string indexName, bool isMapReduce)
		{
			throw new NotSupportedException("The async sharded document store doesn't support synchronous operations");
		}

		protected override IAsyncDocumentQuery<T> IDocumentQueryGeneratorAsyncQuery<T>(string indexName, bool isMapReduce)
		{
			return AsyncLuceneQuery<T>(indexName);
		}

		public Task<IEnumerable<T>> LoadStartingWithAsync<T>(string keyPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null, RavenPagingInformation pagingInformation = null)
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
			}, (dbCmd, i) => dbCmd.StartsWithAsync(keyPrefix, matches, start, pageSize, exclude: exclude))
								.ContinueWith(task => (IEnumerable<T>)task.Result.SelectMany(x => x).Select(TrackEntity<T>).ToList());
		}

		public Task<IEnumerable<TResult>> LoadStartingWithAsync<TTransformer, TResult>(string keyPrefix, string matches = null, int start = 0, int pageSize = 25,
		                                                    string exclude = null, RavenPagingInformation pagingInformation = null,
		                                                    Action<ILoadConfiguration> configure = null) where TTransformer : AbstractTransformerCreationTask, new()
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

			return shardStrategy.ShardAccessStrategy.ApplyAsync(shards, new ShardRequestData
			{
				EntityType = typeof(TResult),
				Keys = { keyPrefix }
			}, (dbCmd, i) => dbCmd.StartsWithAsync(keyPrefix, matches, start, pageSize, exclude: exclude, transformer: transformer,
														 queryInputs: configuration.QueryInputs))
								.ContinueWith(task => (IEnumerable<TResult>)task.Result.SelectMany(x => x).Select(TrackEntity<TResult>).ToList());
		}

		/// <summary>
		/// Queries the index specified by <typeparamref name="TIndexCreator"/> using lucene syntax.
		/// </summary>
		/// <typeparam name="T">The result of the query</typeparam>
		/// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
		/// <returns></returns>
		public IAsyncDocumentQuery<T> AsyncLuceneQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
		{
			var index = new TIndexCreator();

			return AsyncLuceneQuery<T>(index.IndexName, index.IsMapReduce);
		}

		public IAsyncDocumentQuery<T> AsyncLuceneQuery<T>(string indexName, bool isMapReduce = false)
		{
			return new AsyncShardedDocumentQuery<T>(this, GetShardsToOperateOn, shardStrategy, indexName, null, null, theListeners.QueryListeners, isMapReduce);
		}

		public IAsyncDocumentQuery<T> AsyncLuceneQuery<T>()
		{
			return AsyncLuceneQuery<T>(GetDynamicIndexName<T>());
		}

		public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncDocumentQuery<T> query)
		{
			return StreamAsync(query, new Reference<QueryHeaderInformation>());
		}

		public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IQueryable<T> query)
		{
			return StreamAsync(query, new Reference<QueryHeaderInformation>());
		}

		public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncDocumentQuery<T> query, Reference<QueryHeaderInformation> queryHeaderInformation)
		{
			throw new NotSupportedException("Streams are currently not supported by sharded document store");
		}


		public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IQueryable<T> query, Reference<QueryHeaderInformation> queryHeaderInformation)
		{
			throw new NotSupportedException("Streams are currently not supported by sharded document store");
		}

		public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(Etag fromEtag, int start = 0, int pageSize = Int32.MaxValue, RavenPagingInformation pagingInformation = null)
		{
			throw new NotSupportedException("Streams are currently not supported by sharded document store");
		}

		public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(string startsWith, string matches = null, int start = 0, int pageSize = Int32.MaxValue, RavenPagingInformation pagingInformation = null)
		{
			throw new NotSupportedException("Streams are currently not supported by sharded document store");
		}

		#endregion

		/// <summary>
		/// Saves all the changes to the Raven server.
		/// </summary>
		Task IAsyncDocumentSession.SaveChangesAsync()
		{
			return asyncDocumentKeyGeneration.GenerateDocumentKeysForSaveChanges()
											 .ContinueWith(keysTask =>
											 {
												 keysTask.AssertNotFailed();

												 var cachingScope = EntityToJson.EntitiesToJsonCachingScope();
												 try
												 {
													 var data = PrepareForSaveChanges();
													 if (data.Commands.Count == 0 && deferredCommandsByShard.Count == 0)
													 {
														 cachingScope.Dispose();
														 return new CompletedTask(); // nothing to do here
													 }

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
															 throw new InvalidOperationException(
																 string.Format("ShardedDocumentStore cannot found a DatabaseCommands for shard id '{0}'.", shardId));

														 var localCopy = shardAndObjects.Value;
														 saveChanges.Add(localCopy);
														 saveTasks.Add(() => databaseCommands.BatchAsync(localCopy.Commands.ToArray()));
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
	}
}
#endif
