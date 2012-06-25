// -----------------------------------------------------------------------
//  <copyright file="ShardedHiloKeyGenerator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Client.Connection;
using Raven.Client.Document;
#if !NET35
namespace Raven.Client.Shard
{
	public class ShardedHiloKeyGenerator
	{
		private readonly ShardedDocumentStore shardedDocumentStore;
		private readonly int capacity;

		private Dictionary<string, MultiTypeHiLoKeyGenerator> generatorsByShard = new Dictionary<string, MultiTypeHiLoKeyGenerator>();

		public ShardedHiloKeyGenerator(ShardedDocumentStore shardedDocumentStore, int capacity)
		{
			this.shardedDocumentStore = shardedDocumentStore;
			this.capacity = capacity;
		}

		public string GenerateDocumentKey(IDatabaseCommands databaseCommands, DocumentConvention conventions, object entity)
		{
			var shardId = shardedDocumentStore.ShardStrategy.ShardResolutionStrategy.MetadataShardIdFor(entity);
			if (shardId == null)
				throw new InvalidOperationException(string.Format(
					"ShardResolutionStrategy.MetadataShardIdFor cannot return null. You must specify where to store the metadata documents for the entity type '{0}'.", entity.GetType().FullName));

			MultiTypeHiLoKeyGenerator value;
			if (generatorsByShard.TryGetValue(shardId, out value))
			{
				
				return value.GenerateDocumentKey(databaseCommands, conventions, entity);
			}

			lock (this)
			{
				if (generatorsByShard.TryGetValue(shardId, out value) == false)
				{
					value = new MultiTypeHiLoKeyGenerator(capacity);
					generatorsByShard = new Dictionary<string, MultiTypeHiLoKeyGenerator>(generatorsByShard)
					                    	{
					                    		{shardId, value}
					                    	};
				}
			}

			return value.GenerateDocumentKey(databaseCommands, conventions, entity);
		}
	}
}
#endif