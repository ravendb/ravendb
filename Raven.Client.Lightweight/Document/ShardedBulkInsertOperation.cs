using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Text;
using System.Threading.Tasks;
using System.Web.WebSockets;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Extensions;
using Raven.Client.Shard;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
	public class ShardedBulkInsertOperation : IBulkInsertOperation, IDisposable
	{

		private readonly GenerateEntityIdOnTheClient generateEntityIdOnTheClient;

		private readonly ShardedDocumentStore shardedDocumentStore;
		public IAsyncDatabaseCommands DatabaseCommands { get; private set; }
		public IDictionary<string, IDocumentStore> Shards;
		private IDictionary<string, IDatabaseCommands> shardDbCommands;

		protected IDictionary<Guid,ILowLevelBulkInsertOperation> Operations { get; set; }

		//key - shardID, Value - bulk
		private IDictionary<string, BulkInsertOperation> bulks { get; set; }

		public ShardedBulkInsertOperation(string database, ShardedDocumentStore shardedDocumentStore, DocumentSessionListeners listeners, BulkInsertOptions options, IDatabaseChanges changes)
		{
			this.shardedDocumentStore = shardedDocumentStore;
			Shards = new Dictionary<string, IDocumentStore>();
			bulks = new Dictionary<string, BulkInsertOperation>();
			/*DatabaseCommands = database == null
				? shardedDocumentStore.AsyncDatabaseCommands.ForSystemDatabase()
				: shardedDocumentStore.AsyncDatabaseCommands.ForDatabase(database);*/

		}
		public Guid OperationId { get; private set; }
		public bool IsAborted
		{
			get
			{
				return bulks.Select(bulk => bulk.Value).Any(bulkOperation => bulkOperation.IsAborted);
			}
		}

		public void Abort()
		{
			foreach (var bulk in bulks)
			{
				var bulkOperation = bulk.Value;
				bulkOperation.Abort();
			}
		}

		public event Action<string> Report;


		/*public Task DisposeAsync()
		{
		/*	var dis = new Task(() => DisposeAsync());
			foreach (var bulk in bulks)
			{
				var bulkOperation = bulk.Value;
				 dis = bulkOperation.DisposeAsync();
			}
			return dis;#1#
			return Task
		}*/


		public Task DisposeAsync()
		{
			throw new NotImplementedException();
		}

		void IBulkInsertOperation.Dispose()
		{
			foreach (var bulk in bulks)
			{
				var bulkOperation = bulk.Value;
				bulkOperation.Dispose();
			}			
		}

		public string Store(object entity)
		{
				var id = GetId(entity);
				Store(entity, id);
				return id;	
		}

		public void Store(object entity, string id)
		{
				var shardId = GetShardId(entity);
				if (shardId == null)
					throw new InvalidOperationException("Cannot store a document when the shard id isn't defined. Missing Raven-Shard-Id in the metadata");
				var bulkInsertOperation = bulks[shardId];
				bulkInsertOperation.Store(entity, id);
			
		}

		public void Store(RavenJObject document, RavenJObject metadata, string id, int? dataSize = null)
		{
			var shardId = metadata.Value<string>(Constants.RavenShardId);
			var bulkInsertOperation = bulks[shardId];
			bulkInsertOperation.Store(document, metadata, id, dataSize);
		}	
	
		void IDisposable.Dispose()
		{
			foreach (var bulk in bulks)
			{
				var bulkOperation = bulk.Value;
				bulkOperation.Dispose();
			}			
		}

		private string GetId(object entity)
		{
			string id;
			if (generateEntityIdOnTheClient.TryGetIdFromInstance(entity, out id) == false)
			{
				id = generateEntityIdOnTheClient.GenerateDocumentKeyForStorage(entity);
			}
			return id;
		}
		private string GetShardId(object entity)
		{
			using (var session = shardedDocumentStore.OpenSession())
			{
				var metadata = session.Advanced.GetMetadataFor(entity);
				var shardId = metadata.Value<string>(Constants.RavenShardId);
				if (shardId == null)
					throw new InvalidOperationException("Cannot store a document when the shard id isn't defined. Missing Raven-Shard-Id in the metadata");
				return shardId;
			}
		}
		/*private Guid GetOperationId(object entity)
		{
			string id;
			if (generateEntityIdOnTheClient.TryGetIdFromInstance(entity, out id) == false)
			{
				id = generateEntityIdOnTheClient.GenerateDocumentKeyForStorage(entity);
			}
			return id;
		}*/
	}
}

