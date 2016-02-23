using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Changes;
using Raven.Client.Extensions;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
    public class BulkInsertOperation : IDisposable
    {
        public Guid OperationId => Operation.OperationId;

        private readonly IDocumentStore documentStore;
        private readonly GenerateEntityIdOnTheClient generateEntityIdOnTheClient;
        protected IBulkInsertOperation Operation { get; set; }
        public IAsyncDatabaseCommands DatabaseCommands { get; private set; }
        private readonly EntityToJson entityToJson;

        public delegate void BeforeEntityInsert(string id, RavenJObject data, RavenJObject metadata);

        public event BeforeEntityInsert OnBeforeEntityInsert = delegate { };

        public bool IsAborted => Operation.IsAborted;

        public void Abort()
        {
            Operation.Abort();
        }

        public event Action<string> Report
        {
            add { Operation.Report += value; }
            remove { Operation.Report -= value; }
        }

        public BulkInsertOperation(string database, IDocumentStore documentStore, DocumentSessionListeners listeners, BulkInsertOptions options, IDatabaseChanges changes)
        {
            this.documentStore = documentStore;

            database = database ?? MultiDatabase.GetDatabaseName(documentStore.Url);

            // Fitzchak: Should not be ever null because of the above code, please refactor this.
            DatabaseCommands = database == null
                ? documentStore.AsyncDatabaseCommands.ForSystemDatabase()
                : documentStore.AsyncDatabaseCommands.ForDatabase(database);

            generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(documentStore.Conventions, entity =>
                AsyncHelpers.RunSync(() => documentStore.Conventions.GenerateDocumentKeyAsync(database, DatabaseCommands, entity)));

            // ReSharper disable once VirtualMemberCallInContructor
            Operation = GetBulkInsertOperation(options, DatabaseCommands, changes);
            entityToJson = new EntityToJson(documentStore, listeners);
        }

        protected virtual IBulkInsertOperation GetBulkInsertOperation(BulkInsertOptions options, IAsyncDatabaseCommands commands, IDatabaseChanges changes)
        {			
            return commands.GetBulkInsertOperation(options, changes);
        }

        public async Task DisposeAsync()
        {
            await Operation.DisposeAsync().ConfigureAwait(false);
        }

        public void Dispose()
        {
            Operation.Dispose();
        }

        public async Task<string> StoreAsync(object entity)
        {
            var id = GetId(entity);
            await StoreAsync(entity, id).ConfigureAwait(false);
            return id;
        }

        public async Task StoreAsync(object entity, string id)
        {
            if(Operation.IsAborted)
                throw new InvalidOperationException("Bulk insert has been aborted or the operation was timed out");

            var metadata = new RavenJObject();
            var tag = documentStore.Conventions.GetDynamicTagName(entity);
            if (tag != null)
                metadata.Add(Constants.RavenEntityName, tag);

            var data = entityToJson.ConvertEntityToJson(id, entity, metadata);
            OnBeforeEntityInsert(id, data, metadata);

            await Operation.WriteAsync(id, metadata, data).ConfigureAwait(false);
        }

        private string GetId(object entity)
        {
            string id;
            if (generateEntityIdOnTheClient.TryGetIdFromInstance(entity, out id) == false)
            {
                id = generateEntityIdOnTheClient.GenerateDocumentKeyForStorage(entity);
                generateEntityIdOnTheClient.TrySetIdentity(entity,id); //set Id property if it was null
            }
            return id;
        }
    }
}
