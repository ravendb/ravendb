using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Raven.Server.Documents.Indexes.Static;
using Sparrow.Json;

namespace FastTests
{
    public static class DocumentStoreExtensions
    {
        public static DatabaseCommands Commands(this DocumentStore store, string databaseName = null)
        {
            return new DatabaseCommands(store, databaseName);
        }

        public class DatabaseCommands : IDisposable
        {
            private readonly DocumentStore _store;
            public readonly RequestExecuter RequestExecuter;

            public readonly JsonOperationContext Context;
            private readonly IDisposable _returnContext;

            public DatabaseCommands(DocumentStore store, string databaseName)
            {
                if (store == null)
                    throw new ArgumentNullException(nameof(store));

                _store = store;

                if (string.IsNullOrWhiteSpace(databaseName) || string.Equals(store.DefaultDatabase, databaseName, StringComparison.OrdinalIgnoreCase))
                    RequestExecuter = store.GetRequestExecuterForDefaultDatabase();
                else
                    RequestExecuter = store.GetRequestExecuter(databaseName);

                _returnContext = RequestExecuter.ContextPool.AllocateOperationContext(out Context);
            }

            public async Task<PutResult> PutAsync(string id, long? etag, object data, IDictionary<string, string> metadata)
            {
                if (id == null)
                    throw new ArgumentNullException(nameof(id));
                if (data == null)
                    throw new ArgumentNullException(nameof(data));

                var documentInfo = new DocumentInfo
                {
                    Id = id,
                    ETag = etag
                };

                if (metadata != null)
                    documentInfo.MetadataInstance = metadata;

                using (var session = _store.OpenSession())
                {
                    var document = session.Advanced.EntityToBlittable.ConvertEntityToBlittable(data, documentInfo);

                    var command = new PutDocumentCommand
                    {
                        Id = id,
                        Etag = etag,
                        Context = Context,
                        Document = document
                    };

                    await RequestExecuter.ExecuteAsync(command, Context);

                    return command.Result;
                }
            }

            public async Task<DynamicBlittableJson> GetAsync(string id)
            {
                if (id == null)
                    throw new ArgumentNullException(nameof(id));

                var command = new GetDocumentCommand
                {
                    Id = id
                };

                await RequestExecuter.ExecuteAsync(command, Context);

                if (command.Result == null || command.Result.Results.Length == 0)
                    return null;

                var json = (BlittableJsonReaderObject)command.Result.Results[0];
                return new DynamicBlittableJson(json);
            }

            public async Task<DynamicArray> GetAsync(string[] ids)
            {
                if (ids == null)
                    throw new ArgumentNullException(nameof(ids));

                var command = new GetDocumentCommand
                {
                    Ids = ids
                };

                await RequestExecuter.ExecuteAsync(command, Context);

                return new DynamicArray(command.Result.Results);
            }

            public async Task<DynamicArray> GetAsync(int start, int pageSize)
            {
                var command = new GetDocumentCommand
                {
                    Start = start,
                    PageSize = pageSize
                };

                await RequestExecuter.ExecuteAsync(command, Context);

                return new DynamicArray(command.Result.Results);
            }

            public void Dispose()
            {
                _returnContext?.Dispose();
            }
        }
    }
}