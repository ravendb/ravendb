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
        public Guid OperationId
        {
            get
            {
                return Operation.OperationId;
            }
        }

        private readonly IDocumentStore documentStore;
        private readonly GenerateEntityIdOnTheClient generateEntityIdOnTheClient;
        protected internal ILowLevelBulkInsertOperation Operation { get; set; }
        public IAsyncDatabaseCommands DatabaseCommands { get; private set; }
        private readonly EntityToJson entityToJson;

        public delegate void BeforeEntityInsert(string id, RavenJObject data, RavenJObject metadata);

        public event BeforeEntityInsert OnBeforeEntityInsert = delegate { };

        public bool IsAborted
        {
            get { return Operation.IsAborted; }
        }

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

            Operation = GetBulkInsertOperation(options, DatabaseCommands, changes);
            entityToJson = new EntityToJson(documentStore, listeners);
        }

        protected virtual ILowLevelBulkInsertOperation GetBulkInsertOperation(BulkInsertOptions options, IAsyncDatabaseCommands commands, IDatabaseChanges changes)
        {
            return commands.GetBulkInsertOperation(options, changes);
        }

        public Task DisposeAsync()
        {
            return Operation.DisposeAsync();
        }

        public Task WaitForLastTaskToFinish()
        {
            return Operation.WaitForLastTaskToFinish();
        }

        public void Dispose()
        {
            Operation.Dispose();
        }

        public string Store(object entity)
        {
            var id = GetId(entity);
            Store(entity, id);
            return id;
        }

        public async Task<string> StoreAsync(object entity)
        {
            var id = GetId(entity);
            await StoreAsync(entity, id).ConfigureAwait(false);
            return id;
        }

        public void Store(object entity, string id)
        {
            if (Operation.IsAborted)
                throw new InvalidOperationException("Bulk insert has been aborted or the operation was timed out");

            var metadata = new RavenJObject();

            var tag = documentStore.Conventions.GetDynamicTagName(entity);
            if (tag != null)
                metadata.Add(Constants.RavenEntityName, tag);

            var data = entityToJson.ConvertEntityToJson(id, entity, metadata);

            OnBeforeEntityInsert(id, data, metadata);

            Operation.Write(id, metadata, data);
        }

        public Task StoreAsync(object entity, string id)
        {
            if (Operation.IsAborted)
                throw new InvalidOperationException("Bulk insert has been aborted or the operation was timed out");

            var metadata = new RavenJObject();

            var tag = documentStore.Conventions.GetDynamicTagName(entity);
            if (tag != null)
                metadata.Add(Constants.RavenEntityName, tag);

            var data = entityToJson.ConvertEntityToJson(id, entity, metadata);

            OnBeforeEntityInsert(id, data, metadata);

            return Operation.WriteAsync(id, metadata, data);
        }

        public void Store(RavenJObject document, RavenJObject metadata, string id, int? dataSize = null)
        {
            OnBeforeEntityInsert(id, document, metadata);

            Operation.Write(id, metadata, document, dataSize);
        }

        public Task StoreAsync(RavenJObject document, RavenJObject metadata, string id, int? dataSize = null)
        {
            OnBeforeEntityInsert(id, document, metadata);

            return Operation.WriteAsync(id, metadata, document, dataSize);
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
    }
}
