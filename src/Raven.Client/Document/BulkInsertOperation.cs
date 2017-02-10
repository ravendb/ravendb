using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Util;

namespace Raven.Client.Document
{
    public class BulkInsertOperation : IDisposable
    {
        private readonly IDocumentStore _store;
        private readonly GenerateEntityIdOnTheClient _generateEntityIdOnTheClient;
        protected TcpBulkInsertOperation Operation { get; set; }

        /*public delegate void BeforeEntityInsert(string id, RavenJObject data, RavenJObject metadata);

        public event BeforeEntityInsert OnBeforeEntityInsert = delegate { };*/

        public void Abort()
        {
            Operation.Abort();
        }

        public event Action<string> Report
        {
            add { Operation.Report += value; }
            remove { Operation.Report -= value; }
        }

        public BulkInsertOperation(string database, IDocumentStore store)
        {
            _store = store;

            database = database ?? MultiDatabase.GetDatabaseName(store.Url);

            _generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(store.Conventions, entity =>
                AsyncHelpers.RunSync(() => store.Conventions.GenerateDocumentKeyAsync(database, entity)));

            // ReSharper disable once VirtualMemberCallInContructor
            Operation = GetBulkInsertOperation(database, store.GetRequestExecuter(database));
        }

        protected virtual TcpBulkInsertOperation GetBulkInsertOperation(string database, RequestExecuter requestExecuter)
        {
            return new TcpBulkInsertOperation(database, _store, requestExecuter, default(CancellationTokenSource));
        }

        public async Task DisposeAsync()
        {
            await Operation.DisposeAsync().ConfigureAwait(false);
        }

        public void Dispose()
        {
            Operation.Dispose();
        }

        public string Store(object entity)
        {
            return AsyncHelpers.RunSync(() => StoreAsync(entity));
        }

        public async Task<string> StoreAsync(object entity)
        {
            var id = GetId(entity);
            await StoreAsync(entity, id).ConfigureAwait(false);
            return id;
        }

        public async Task StoreAsync(object entity, string id)
        {
            await Operation.WriteAsync(id, entity).ConfigureAwait(false);
        }

        private string GetId(object entity)
        {
            string id;
            if (_generateEntityIdOnTheClient.TryGetIdFromInstance(entity, out id) == false)
            {
                id = _generateEntityIdOnTheClient.GenerateDocumentKeyForStorage(entity);
                _generateEntityIdOnTheClient.TrySetIdentity(entity, id); //set Id property if it was null
            }
            return id;
        }
    }
}
