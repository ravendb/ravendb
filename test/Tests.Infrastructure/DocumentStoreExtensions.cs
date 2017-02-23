using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents.Indexes.Static;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace FastTests
{
    public static class DocumentStoreExtensions
    {
        public static DatabaseCommands Commands(this IDocumentStore store, string databaseName = null)
        {
            return new DatabaseCommands(store, databaseName);
        }

        public class DatabaseCommands : IDisposable
        {
            private readonly IDocumentStore _store;
            public readonly RequestExecuter RequestExecuter;

            public readonly JsonOperationContext Context;
            private readonly IDisposable _returnContext;

            public DatabaseCommands(IDocumentStore store, string databaseName)
            {
                if (store == null)
                    throw new ArgumentNullException(nameof(store));

                _store = store;

                RequestExecuter = store.GetRequestExecuter(databaseName);

                _returnContext = RequestExecuter.ContextPool.AllocateOperationContext(out Context);
            }

            public BlittableJsonReaderObject ParseJson(string json)
            {
                using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(json)))
                {
                    return Context.ReadForMemory(stream, "json");
                }
            }

            public TEntity Deserialize<TEntity>(BlittableJsonReaderObject json)
            {
                return (TEntity)_store.Conventions.DeserializeEntityFromBlittable(typeof(TEntity), json);
            }

            public PutResult Put(string id, long? etag, object data, Dictionary<string, string> metadata)
            {
                return AsyncHelpers.RunSync(() => PutAsync(id, etag, data, metadata));
            }

            public async Task<PutResult> PutAsync(string id, long? etag, object data, Dictionary<string, string> metadata, CancellationToken cancellationToken = default(CancellationToken))
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

                using (var session = _store.OpenSession())
                {
                    var documentJson = data as BlittableJsonReaderObject;
                    var metadataJson = metadata != null
                        ? session.Advanced.EntityToBlittable.ConvertEntityToBlittable(metadata, session.Advanced.DocumentStore.Conventions, session.Advanced.Context)
                        : null;

                    if (documentJson == null)
                    {
                        if (metadataJson != null)
                        {
                            documentInfo.Metadata = metadataJson;
                            documentInfo.MetadataInstance = metadata;
                        }

                        documentJson = session.Advanced.EntityToBlittable.ConvertEntityToBlittable(data, documentInfo);
                    }
                    else
                    {
                        if (metadataJson != null)
                        {
                            documentJson.Modifications = new DynamicJsonValue(documentJson)
                            {
                                [Constants.Documents.Metadata.Key] = metadataJson
                            };

                            documentJson = session.Advanced.Context.ReadObject(documentJson, id);
                        }
                    }


                    var command = new PutDocumentCommand
                    {
                        Id = id,
                        Etag = etag,
                        Context = Context,
                        Document = documentJson
                    };

                    await RequestExecuter.ExecuteAsync(command, Context, cancellationToken);

                    return command.Result;
                }
            }

            public void Delete(string id, long? etag)
            {
                AsyncHelpers.RunSync(() => DeleteAsync(id, etag));
            }

            public async Task DeleteAsync(string id, long? etag)
            {
                if (id == null)
                    throw new ArgumentNullException(nameof(id));

                var command = new DeleteDocumentCommand(id, etag);

                await RequestExecuter.ExecuteAsync(command, Context);
            }

            public long? Head(string id)
            {
                return AsyncHelpers.RunSync(() => HeadAsync(id));
            }

            public async Task<long?> HeadAsync(string id)
            {
                if (id == null)
                    throw new ArgumentNullException(nameof(id));

                var command = new HeadDocumentCommand(id, null);

                await RequestExecuter.ExecuteAsync(command, Context);

                return command.Result;
            }          

            public GetConflictsResult.Conflict[] GetConflictsFor(string id)
            {
                var getConflictsCommand = new GetConflictsCommand(id);
                RequestExecuter.Execute(getConflictsCommand, Context);

                return getConflictsCommand.Result.Results;
            }

            public DynamicBlittableJson Get(string id, bool metadataOnly = false)
            {
                return AsyncHelpers.RunSync(() => GetAsync(id, metadataOnly));
            }

            public async Task<DynamicBlittableJson> GetAsync(string id, bool metadataOnly = false)
            {
                if (id == null)
                    throw new ArgumentNullException(nameof(id));

                var command = new GetDocumentCommand
                {
                    Id = id,
                    MetadataOnly = metadataOnly
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

            public QueryResult Query(string indexName, IndexQuery query, bool metadataOnly = false, bool indexEntriesOnly = false)
            {
                return AsyncHelpers.RunSync(() => QueryAsync(indexName, query, metadataOnly, indexEntriesOnly));
            }

            public async Task<QueryResult> QueryAsync(string indexName, IndexQuery query, bool metadataOnly = false, bool indexEntriesOnly = false)
            {
                var command = new QueryCommand(_store.Conventions, Context, indexName, query, metadataOnly: metadataOnly, indexEntriesOnly: indexEntriesOnly);

                await RequestExecuter.ExecuteAsync(command, Context);

                return command.Result;
            }

            public void Batch(List<ICommandData> commands)
            {
                AsyncHelpers.RunSync(() => BatchAsync(commands));
            }

            public async Task BatchAsync(List<ICommandData> commands)
            {
                var command = new BatchCommand(Context, commands);

                await RequestExecuter.ExecuteAsync(command, Context);
            }

            public TResult RawGetJson<TResult>(string url)
                where TResult : BlittableJsonReaderBase
            {
                return AsyncHelpers.RunSync(() => RawGetJsonAsync<TResult>(url));
            }

            public async Task<TResult> RawGetJsonAsync<TResult>(string url) 
                where TResult : BlittableJsonReaderBase
            {
                var command = new GetJsonCommand<TResult>(url);

                await RequestExecuter.ExecuteAsync(command, Context);

                return command.Result;
            }

            public void Dispose()
            {
                _returnContext?.Dispose();
            }

            private class GetJsonCommand<TResult> : RavenCommand<TResult>
                where TResult : BlittableJsonReaderBase
            {
                private readonly string _url;

                public GetJsonCommand(string url)
                {
                    if (url == null)
                        throw new ArgumentNullException(nameof(url));

                    if (url.StartsWith("/") == false)
                        url += $"/{url}";

                    _url = url;

                    if (typeof(TResult) == typeof(BlittableJsonReaderArray))
                        ResponseType = RavenCommandResponseType.Array;
                }

                public override bool IsReadRequest => true;

                public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
                {
                    url = $"{node.Url}/databases/{node.Database}{_url}";

                    var request = new HttpRequestMessage
                    {
                        Method = HttpMethod.Get
                    };

                    return request;
                }

                public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
                {
                    Result = (TResult)(object)response;
                }

                public override void SetResponse(BlittableJsonReaderArray response, bool fromCache)
                {
                    Result = (TResult)(object)response;
                }
            }
        }
    }
}