using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection.Async;

namespace Raven.Client.Document.Async
{
    public class NagleAsyncDocumentSession : AsyncDocumentSession
    {
        private readonly DocumentStore documentStore;

        public NagleAsyncDocumentSession(string dbName, DocumentStore documentStore, IAsyncDatabaseCommands asyncDatabaseCommands, DocumentSessionListeners listeners, Guid id) : base(dbName, documentStore, asyncDatabaseCommands, listeners, id)
        {
            this.documentStore = documentStore;
        }

        public override async Task SaveChangesAsync(CancellationToken token = default(CancellationToken))
        {
            await asyncDocumentKeyGeneration.GenerateDocumentKeysForSaveChanges().WithCancellation(token).ConfigureAwait(false);

            using (EntityToJson.EntitiesToJsonCachingScope())
            {
                var data = PrepareForSaveChanges();
                if (data.Commands.Count == 0)
                    return;

                LogBatch(data);

                var task = documentStore.AddNagleData(data);
                var result = await task.ConfigureAwait(false);
                UpdateBatchResults(result, data);
            }
        }
    }
}