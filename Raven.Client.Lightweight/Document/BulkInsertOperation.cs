using System;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
	public class BulkInsertOperation : IDisposable
	{
		private readonly IDocumentStore documentStore;
		private readonly GenerateEntityIdOnTheClient generateEntityIdOnTheClient;
		private readonly ILowLevelBulkInsertOperation operation;
		private readonly IDatabaseCommands databaseCommands;
		private readonly EntityToJson entityToJson;

		public delegate void BeforeEntityInsert(string id, RavenJObject data, RavenJObject metadata);

		public event BeforeEntityInsert OnBeforeEntityInsert = delegate { }; 

		public event Action<string>  Report
		{
			add { operation.Report += value; }
			remove { operation.Report -= value; }
		}

		public BulkInsertOperation(string database, IDocumentStore documentStore, DocumentSessionListeners listeners, BulkInsertOptions options)
		{
			this.documentStore = documentStore;
			databaseCommands = database == null
				                   ? documentStore.DatabaseCommands.ForDefaultDatabase()
				                   : documentStore.DatabaseCommands.ForDatabase(database);

			generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(documentStore, entity => documentStore.Conventions.GenerateDocumentKey(database, databaseCommands, entity));
			operation = databaseCommands.GetBulkInsertOperation(options);
			entityToJson = new EntityToJson(documentStore, listeners);
		}

		public void Dispose()
		{
			operation.Dispose();
		}

		public void Store(object entity)
		{
			Store(entity, GetId(entity));
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