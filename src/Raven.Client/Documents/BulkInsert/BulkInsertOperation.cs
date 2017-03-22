using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Exceptions.BulkInsert;
using Raven.Client.Documents.Identity;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Server;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.BulkInsert
{
    public class BulkInsertOperation : IDisposable
    {
        private readonly IDocumentStore _store;
        private readonly GenerateEntityIdOnTheClient _generateEntityIdOnTheClient;

        private class StreamExposerContent : HttpContent
        {
            public readonly Task<Stream> OutputStream;
            private readonly TaskCompletionSource<Stream> _outputStreamTcs;
            private readonly TaskCompletionSource<object> _done;

            public StreamExposerContent()
            {
                _outputStreamTcs = new TaskCompletionSource<Stream>();
                OutputStream = _outputStreamTcs.Task;
                _done = new TaskCompletionSource<object>();
            }

            public void Done()
            {
                _done.TrySetResult(null);
            }

            protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
            {
                _outputStreamTcs.SetResult(stream);
                return _done.Task;
            }

            protected override bool TryComputeLength(out long length)
            {
                length = -1;
                return false;
            }
        }

        private class BulkInsertCommand : RavenCommand<HttpResponseMessage>
        {
            public override bool IsReadRequest => false;
            private readonly StreamExposerContent _stream;
            public Task<HttpResponseMessage> ExecutionTask { get; private set; }

            public BulkInsertCommand(StreamExposerContent stream)
            {
                _stream = stream;
                Timeout = TimeSpan.FromHours(12); // global max timeout
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/bulk_insert";
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }

            public override Task<HttpResponseMessage> SendAsync(HttpClient client, HttpRequestMessage request, CancellationToken token)
            {
                ExecutionTask = client.PostAsync(request.RequestUri, _stream, token);
                return ExecutionTask;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                throw new NotImplementedException();
            }
        }

        private readonly JsonOperationContext _context;
        private readonly Task _bulkInsertExecuteTask;
        private readonly EntityToBlittable _convertor = new EntityToBlittable(null);
        private readonly IDisposable _dispose;

        private BlittableJsonTextWriter _jsonWriter;
        private Stream _stream;
        private readonly StreamExposerContent _streamExposerContent;
        private bool _first = true;
        private bool _justDispose;
        private readonly BulkInsertCommand _bulkCommand;

        public BulkInsertOperation(string database, IDocumentStore store, CancellationToken token = default(CancellationToken))
        {
            _store = store;
            database = database ?? MultiDatabase.GetDatabaseName(store.Url);

            var requestExecuter = _store.GetRequestExecuter(database);
            _dispose = requestExecuter.ContextPool.AllocateOperationContext(out _context);
            _streamExposerContent = new StreamExposerContent();
            _bulkCommand = new BulkInsertCommand(_streamExposerContent);
            _bulkInsertExecuteTask = requestExecuter.ExecuteAsync(_bulkCommand, _context, token);

            _generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(store.Conventions, entity =>
                AsyncHelpers.RunSync(() => store.Conventions.GenerateDocumentIdAsync(database, entity)));
        }

        public async Task DisposeAsync()
        {
            try
            {
                if (_justDispose == false)
                {
                    if (_stream != null)
                    {
                        _jsonWriter.WriteEndArray();
                        _jsonWriter.Flush();
                        await _stream.FlushAsync().ConfigureAwait(false);
                    }

                    _streamExposerContent.Done();

                    await _bulkInsertExecuteTask.ConfigureAwait(false);
                }
            }
            finally
            {
                _streamExposerContent?.Dispose();
                _dispose.Dispose();
            }
        }

        public void Dispose()
        {
            AsyncHelpers.RunSync(DisposeAsync);
        }

        public void Store(object entity, string id)
        {
            AsyncHelpers.RunSync(() => StoreAsync(entity, id));
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
            if (_stream == null)
            {
                // either the server is down or we get the stream. 
                var completed = Task.WaitAny(_streamExposerContent.OutputStream, _bulkCommand.ExecutionTask);
                if (completed == 1 && _bulkCommand.ExecutionTask.Status == TaskStatus.Faulted)
                {
                    ThrowOnUnavailableStream(id);
                }
                _stream = await _streamExposerContent.OutputStream.ConfigureAwait(false);
                _jsonWriter = new BlittableJsonTextWriter(_context, _stream);
                _jsonWriter.WriteStartArray();
            }

            using (var doc = _convertor.ConvertEntityToBlittable(entity, _store.Conventions, _context, new DocumentInfo
            {
                Collection = _store.Conventions.GetCollectionName(entity)
            }))
            {
                if (_first == false)
                {
                    _jsonWriter.WriteComma();
                }
                _first = false;

                var cmd = new DynamicJsonValue
                {
                    [nameof(PutCommandDataWithBlittableJson.Method)] = "PUT",
                    [nameof(PutCommandDataWithBlittableJson.Key)] = id,
                    [nameof(PutCommandDataWithBlittableJson.Document)] = doc,
                };

                try
                {
                    _context.Write(_jsonWriter, cmd);
                }
                catch (Exception e)
                {
                    ThrowOnUnavailableStream(id,e);
                }
            }
        }

        private void ThrowOnUnavailableStream(string id, Exception innerEx = null)
        {
            _stream = null;
            _justDispose = true;            
            throw new BulkInsertAbortedException($"Write to stream faild at {id}", innerEx);
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
