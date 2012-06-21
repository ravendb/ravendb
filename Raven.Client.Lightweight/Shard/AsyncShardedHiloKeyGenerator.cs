#if !NET35
// -----------------------------------------------------------------------
//  <copyright file="ShardedHiloKeyGenerator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Connection.Async;
using Raven.Client.Document;

namespace Raven.Client.Shard
{
	public class AsyncShardedHiloKeyGenerator
	{
		private readonly ShardedDocumentStore shardedDocumentStore;
		private readonly int capacity;

		private Dictionary<string, AsyncMultiTypeHiLoKeyGenerator> generatorsByShard = new Dictionary<string, AsyncMultiTypeHiLoKeyGenerator>();

		public AsyncShardedHiloKeyGenerator(ShardedDocumentStore shardedDocumentStore, int capacity)
		{
			this.shardedDocumentStore = shardedDocumentStore;
			this.capacity = capacity;
		}

		public Task<string> GenerateDocumentKeyAsync(IAsyncDatabaseCommands databaseCommands, DocumentConvention conventions, object entity)
		{
			var shardId = shardedDocumentStore.ShardStrategy.ShardResolutionStrategy.MetadataShardIdFor(entity);
			if (shardId == null)
				throw new InvalidOperationException(string.Format(
					"ShardResolutionStrategy.MetadataShardIdFor cannot return null. You must specify where to store the metadata documents for the entity type '{0}'.", entity.GetType().FullName));

			AsyncMultiTypeHiLoKeyGenerator value;
			if (generatorsByShard.TryGetValue(shardId, out value))
				return value.GenerateDocumentKeyAsync(databaseCommands, conventions, entity);

			lock (this)
			{
				if (generatorsByShard.TryGetValue(shardId, out value) == false)
				{
					value = new AsyncMultiTypeHiLoKeyGenerator(capacity);
					generatorsByShard = new Dictionary<string, AsyncMultiTypeHiLoKeyGenerator>(generatorsByShard)
					                    	{
					                    		{shardId, value}
					                    	};
				}
			}

			return value.GenerateDocumentKeyAsync(databaseCommands, conventions, entity);
		}
	}
}
#endif