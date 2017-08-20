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
using Raven.Client.Json;
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
            public readonly RequestExecutor RequestExecutor;

            public readonly JsonOperationContext Context;
            private readonly IDisposable _returnContext;

            public DatabaseCommands(IDocumentStore store, string databaseName)
            {
                _store = store ?? throw new ArgumentNullException(nameof(store));

                RequestExecutor = store.GetRequestExecutor(databaseName);

                _returnContext = RequestExecutor.ContextPool.AllocateOperationContext(out Context);
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

            public PutResult Put(string id, string changeVector, object data, Dictionary<string, object> metadata = null)
            {
                return AsyncHelpers.RunSync(() => PutAsync(id, changeVector, data, metadata));
            }

            public async Task<PutResult> PutAsync(string id, string changeVector, object data, Dictionary<string, object> metadata = null, CancellationToken cancellationToken = default(CancellationToken))
            {
                if (id == null)
                    throw new ArgumentNullException(nameof(id));
                if (data == null)
                    throw new ArgumentNullException(nameof(data));

                var documentInfo = new DocumentInfo
                {
                    Id = id,
                    ChangeVector = changeVector
                };

                using (var session = _store.OpenSession())
                {
                    var documentJson = data as BlittableJsonReaderObject;
                    var metadataJson = metadata != null
                        ? EntityToBlittable.ConvertEntityToBlittable(metadata, session.Advanced.DocumentStore.Conventions, session.Advanced.Context)
                        : null;

                    if (documentJson == null)
                    {
                        if (metadataJson != null)
                        {
                            documentInfo.Metadata = metadataJson;
                            documentInfo.MetadataInstance = new MetadataAsDictionary(metadata);
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


                    var command = new PutDocumentCommand(id, changeVector, documentJson);

                    await RequestExecutor.ExecuteAsync(command, Context, cancellationToken);

                    return command.Result;
                }
            }

            public void Delete(string id, string changeVector)
            {
                AsyncHelpers.RunSync(() => DeleteAsync(id, changeVector));
            }

            public async Task DeleteAsync(string id, string changeVector)
            {
                if (id == null)
                    throw new ArgumentNullException(nameof(id));

                var command = new DeleteDocumentCommand(id, changeVector);

                await RequestExecutor.ExecuteAsync(command, Context);
            }

            public string Head(string id)
            {
                return AsyncHelpers.RunSync(() => HeadAsync(id));
            }

            public async Task<string> HeadAsync(string id)
            {
                if (id == null)
                    throw new ArgumentNullException(nameof(id));

                var command = new HeadDocumentCommand(id, null);

                await RequestExecutor.ExecuteAsync(command, Context);

                return command.Result;
            }

            public GetConflictsResult.Conflict[] GetConflictsFor(string id)
            {
                var getConflictsCommand = new GetConflictsCommand(id);
                RequestExecutor.Execute(getConflictsCommand, Context);
                return getConflictsCommand.Result.Results;
            }

            public async Task<GetConflictsResult.Conflict[]> GetConflictsForAsync(string id)
            {
                var getConflictsCommand = new GetConflictsCommand(id);
                await RequestExecutor.ExecuteAsync(getConflictsCommand, Context);
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

                var command = new GetDocumentCommand(id, includes: null,metadataOnly: metadataOnly);

                await RequestExecutor.ExecuteAsync(command, Context);

                if (command.Result == null || command.Result.Results.Length == 0)
                    return null;

                var json = (BlittableJsonReaderObject)command.Result.Results[0];
                return new DynamicBlittableJson(json);
            }

            public DynamicArray GetRevisionsFor(string id, int? start = null, int? pageSize = null)
            {
                return AsyncHelpers.RunSync(() => GetRevisionsForAsync(id, start, pageSize));
            }

            public async Task<DynamicArray> GetRevisionsForAsync(string id, int? start = null, int? pageSize = null, bool metadataOnly = false)
            {
                if (id == null)
                    throw new ArgumentNullException(nameof(id));

                var command = new GetRevisionsCommand(id, start, pageSize, metadataOnly);
                await RequestExecutor.ExecuteAsync(command, Context);
                return new DynamicArray(command.Result.Results);
            }

            public DynamicArray GetRevisionsBinEntries(long etag, int? pageSize = null)
            {
                return AsyncHelpers.RunSync(() => GetRevisionsBinEntriesAsync(etag, pageSize));
            }

            public async Task<DynamicArray> GetRevisionsBinEntriesAsync(long etag, int? pageSize = null)
            {
                var command = new GetRevisionsBinEntryCommand(etag, pageSize);
                await RequestExecutor.ExecuteAsync(command, Context);
                return new DynamicArray(command.Result.Results);
            }

            public async Task<DynamicArray> GetAsync(string[] ids)
            {
                if (ids == null)
                    throw new ArgumentNullException(nameof(ids));

                var command = new GetDocumentCommand(ids, includes: null, transformer: null, transformerParameters: null, metadataOnly: false);

                await RequestExecutor.ExecuteAsync(command, Context);

                return new DynamicArray(command.Result.Results);
            }

            public async Task<DynamicArray> GetAsync(int start, int pageSize)
            {
                var command = new GetDocumentCommand(start, pageSize);

                await RequestExecutor.ExecuteAsync(command, Context);

                return new DynamicArray(command.Result.Results);
            }

            public QueryResult Query(IndexQuery query, bool metadataOnly = false, bool indexEntriesOnly = false)
            {
                return AsyncHelpers.RunSync(() => QueryAsync(query, metadataOnly, indexEntriesOnly));
            }

            public async Task<QueryResult> QueryAsync(IndexQuery query, bool metadataOnly = false, bool indexEntriesOnly = false)
            {
                var command = new QueryCommand(_store.Conventions, query, metadataOnly: metadataOnly, indexEntriesOnly: indexEntriesOnly);

                await RequestExecutor.ExecuteAsync(command, Context);

                return command.Result;
            }

            public void Batch(List<ICommandData> commands)
            {
                AsyncHelpers.RunSync(() => BatchAsync(commands));
            }

            public async Task BatchAsync(List<ICommandData> commands)
            {
                var command = new BatchCommand(_store.Conventions, Context, commands);

                await RequestExecutor.ExecuteAsync(command, Context);
            }

            public TResult RawGetJson<TResult>(string url)
                where TResult : BlittableJsonReaderBase
            {
                return AsyncHelpers.RunSync(() => RawGetJsonAsync<TResult>(url));
            }

            public TResult RawDeleteJson<TResult>(string url, object payload)
                where TResult : BlittableJsonReaderBase
            {
                return AsyncHelpers.RunSync(() => RawDeleteJsonAsync<TResult>(url, payload));
            }

            public void ExecuteJson(string url, HttpMethod method, object payload)
            {
                AsyncHelpers.RunSync(() => ExecuteJsonAsync(url, method, payload));
            }

            public async Task<TResult> RawGetJsonAsync<TResult>(string url)
                where TResult : BlittableJsonReaderBase
            {
                var command = new GetJsonCommand<TResult>(url);

                await RequestExecutor.ExecuteAsync(command, Context);

                return command.Result;
            }

            public async Task<TResult> RawDeleteJsonAsync<TResult>(string url, object payload)
                where TResult : BlittableJsonReaderBase
            {
                using (var session = _store.OpenSession())
                {
                    var payloadJson = EntityToBlittable.ConvertEntityToBlittable(payload, session.Advanced.DocumentStore.Conventions, session.Advanced.Context);

                    var command = new JsonCommandWithPayload<TResult>(url, HttpMethod.Delete, payloadJson);

                    await RequestExecutor.ExecuteAsync(command, Context);

                    return command.Result;
                }
            }

            public async Task ExecuteJsonAsync(string url, HttpMethod method, object payload)
            {
                using (var session = _store.OpenSession())
                {
                    BlittableJsonReaderObject payloadJson = null;
                    if (payload != null)
                        payloadJson = EntityToBlittable.ConvertEntityToBlittable(payload, session.Advanced.DocumentStore.Conventions, session.Advanced.Context);

                    var command = new JsonCommandWithPayload<BlittableJsonReaderObject>(url, method, payloadJson);

                    await RequestExecutor.ExecuteAsync(command, Context);
                }
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
                        throw new NotSupportedException("Use object insetad");
                }

                public override bool IsReadRequest => true;

                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
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
            }

            private class JsonCommandWithPayload<TResult> : RavenCommand<TResult>
                where TResult : BlittableJsonReaderBase
            {
                private readonly string _url;
                private readonly HttpMethod _method;
                private readonly BlittableJsonReaderObject _payload;

                public JsonCommandWithPayload(string url, HttpMethod method, BlittableJsonReaderObject payload)
                {
                    if (url == null)
                        throw new ArgumentNullException(nameof(url));

                    if (url.StartsWith("/") == false)
                        url = $"/{url}";

                    _url = url;
                    _method = method;
                    _payload = payload;

                    if (typeof(TResult) == typeof(BlittableJsonReaderArray))
                        throw new NotSupportedException("Use object instead");
                }

                public override bool IsReadRequest => false;

                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    url = $"{node.Url}/databases/{node.Database}{_url}";

                    var request = new HttpRequestMessage
                    {
                        Method = _method,
                        Content = new BlittableJsonContent(stream =>
                        {
                            if (_payload != null)
                                ctx.Write(stream, _payload);
                        })
                    };

                    return request;
                }

                public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
                {
                    Result = (TResult)(object)response;
                }
            }

            public Task ExecuteAsync<TResult>(RavenCommand<TResult> command, CancellationToken cancellationToken = default(CancellationToken))
            {
                return RequestExecutor.ExecuteAsync(command, Context, cancellationToken);
            }

            public void Execute<TResult>(RavenCommand<TResult> command, CancellationToken cancellationToken = default(CancellationToken))
            {
                RequestExecutor.Execute(command, Context, cancellationToken);
            }
        }
    }
}
