using Raven.Client.Connection.Async;
#if !NETFX_CORE
using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Client.Extensions;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
	public class BulkInsertOperation : IDisposable
	{
		public Guid OperationId
		{
			get
			{
				return operation.OperationId;
			}
		}

		private readonly IDocumentStore documentStore;
		private readonly GenerateEntityIdOnTheClient generateEntityIdOnTheClient;
		private readonly ILowLevelBulkInsertOperation operation;
		public IAsyncDatabaseCommands DatabaseCommands { get; private set; }
		private readonly EntityToJson entityToJson;

		public delegate void BeforeEntityInsert(string id, RavenJObject data, RavenJObject metadata);

		public event BeforeEntityInsert OnBeforeEntityInsert = delegate { };

		public event Action<string> Report
		{
			add { operation.Report += value; }
			remove { operation.Report -= value; }
		}

		public BulkInsertOperation(string database, IDocumentStore documentStore, DocumentSessionListeners listeners, BulkInsertOptions options, IDatabaseChanges changes)
		{
			this.documentStore = documentStore;

			database = database ?? MultiDatabase.GetDatabaseName(documentStore.Url);

			// Fitzchak: Should not be ever null because of the above code, please refactor this.
			DatabaseCommands = database == null
				? documentStore.AsyncDatabaseCommands.ForSystemDatabase()
				: documentStore.AsyncDatabaseCommands.ForDatabase(database);

			generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(documentStore, entity => documentStore.Conventions.GenerateDocumentKeyAsync(database, DatabaseCommands, entity).ResultUnwrap());
			operation = DatabaseCommands.GetBulkInsertOperation(options, changes);
			entityToJson = new EntityToJson(documentStore, listeners);
		}

		public Task DisposeAsync()
		{
			return operation.DisposeAsync();
		}

		public void Dispose()
		{
			operation.Dispose();
		}

		public string Store(object entity)
		{
			var id = GetId(entity);
			Store(entity, id);
			return id;
		}

		public void Store(object entity, string id)
		{
			var metadata = new RavenJObject();

			var tag = documentStore.Conventions.GetTypeTagName(entity.GetType());
			if (tag != null)
				metadata.Add(Constants.RavenEntityName, tag);

			var data = entityToJson.ConvertEntityToJson(id, entity, metadata);

			OnBeforeEntityInsert(id, data, metadata);

			operation.Write(id, metadata, data);
		}

		public void Store(RavenJObject document, RavenJObject metadata, string id)
		{
			OnBeforeEntityInsert(id, document, metadata);

			operation.Write(id, metadata, document);
		}

		private string GetId(object entity)
		{
			string id;
			if (generateEntityIdOnTheClient.TryGetIdFromInstance(entity, out id))
			{
				id = generateEntityIdOnTheClient.GenerateDocumentKeyForStorage(entity);
			}
			else
			{
				id = generateEntityIdOnTheClient.GenerateDocumentKeyForStorage(entity);
				generateEntityIdOnTheClient.TrySetIdentity(entity, id);
			}
			return id;
		}
	}
}
#endif