using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
	public class ServerBulkInsert : IBulkInsertOperation
	{
		private readonly IDocumentStore documentStore;
		private readonly GenerateEntityIdOnTheClient generateEntityIdOnTheClient;
		private readonly RemoteBulkInsertOperation operation;
		private readonly IDatabaseCommands databaseCommands;
		public EntityToJson EntityToJson { get; private set; }

		public ServerBulkInsert(string database, IDocumentStore documentStore, int batchSize, DocumentSessionListeners listeners)
		{
			this.documentStore = documentStore;
			databaseCommands = database == null
				                   ? documentStore.DatabaseCommands.ForDefaultDatabase()
				                   : documentStore.DatabaseCommands.ForDatabase(database);

			generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(documentStore, entity => documentStore.Conventions.GenerateDocumentKey(databaseCommands, entity));
			operation = new RemoteBulkInsertOperation(new BulkInsertOptions(),  (ServerClient) databaseCommands);
			EntityToJson = new EntityToJson(documentStore, listeners);
		}

		public void Dispose()
		{
			operation.Dispose();
		}

		public void Store(object entity)
		{
			string id = GetId(entity);

			var metadata = new RavenJObject();

			var tag = documentStore.Conventions.GetTypeTagName(entity.GetType());
			if (tag != null)
				metadata.Add(Constants.RavenEntityName, tag);

			var data = EntityToJson.ConvertEntityToJson(id, entity, metadata);
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