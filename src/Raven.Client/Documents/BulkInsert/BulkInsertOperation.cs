using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Exceptions.BulkInsert;
using Raven.Client.Documents.Identity;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.Server;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.BulkInsert
{
    public class BulkInsertOperation : IDisposable
    {
        private readonly CancellationToken _token;
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
                if (_done.TrySetResult(null) == false)
                {
                    throw new BulkInsertProtocolViolationException("Unable to close the stream");
                }
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

            public void ErrorOnRequestStart(Exception exception)
            {
                _outputStreamTcs.TrySetException(exception);
            }

            public void ErrorOnProcessingRequest(Exception exception)
            {
                _done.TrySetException(exception);
            }
        }
        
        private class BulkInsertCommand : RavenCommand<HttpResponseMessage>
        {
            public override bool IsReadRequest => false;
            private readonly StreamExposerContent _stream;
            private readonly long _id;
            public BulkInsertCommand(long id, StreamExposerContent stream)
            {
                _stream = stream;
                _id = id;
                Timeout = TimeSpan.FromHours(12); // global max timeout
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/bulk_insert?id={_id}";
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }

            public override async Task<HttpResponseMessage> SendAsync(HttpClient client, HttpRequestMessage request, CancellationToken token)
            {
                try
                {
                    return await client.PostAsync(request.RequestUri, _stream, token).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    _stream.ErrorOnRequestStart(e);
                    throw;
                }
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                throw new NotImplementedException();
            }
        }
        private readonly RequestExecutor _requestExecuter;
        private Task _bulkInsertExecuteTask;

        private readonly JsonOperationContext _context;
        private readonly EntityToBlittable _convertor = new EntityToBlittable(null);
        private readonly IDisposable _resetContext;

        private BlittableJsonTextWriter _jsonWriter;
        private Stream _stream;
        private readonly StreamExposerContent _streamExposerContent;

        private bool _first = true;
        private long _operationId = -1;
        private readonly DocumentConventions _conventions;
        
        public BulkInsertOperation(string database, IDocumentStore store, CancellationToken token = default(CancellationToken))
        {
            _token = token;
            database = database ?? MultiDatabase.GetDatabaseName(store.Url);
            _conventions = store.Conventions;
            _requestExecuter = store.GetRequestExecuter(database);
            _resetContext = _requestExecuter.ContextPool.AllocateOperationContext(out _context);
            _streamExposerContent = new StreamExposerContent();

            _generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(store.Conventions, entity =>
                AsyncHelpers.RunSync(() => store.Conventions.GenerateDocumentIdAsync(database, entity)));           
        }

        public long GetOperationId()
        {
            if(_operationId == -1)
                AsyncHelpers.RunSync(WaitForId);
            return _operationId;
        }

        private async Task WaitForId()
        {
            if (_operationId != -1)
                return;

            var bulkInsertGetIdRequest = new GetNextOperationIdCommand();
            await _requestExecuter.ExecuteAsync(bulkInsertGetIdRequest, _context, _token).ConfigureAwait(false);
            _operationId = bulkInsertGetIdRequest.Result;
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
                await WaitForId().ConfigureAwait(false);
                await EnsureStream().ConfigureAwait(false);
            }

            JsonOperationContext tempContext;
            using(_requestExecuter.ContextPool.AllocateOperationContext(out tempContext))
            using (var doc = _convertor.ConvertEntityToBlittable(entity, _conventions, tempContext, new DocumentInfo
            {
                Collection = _conventions.GetCollectionName(entity)
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
                    tempContext.Write(_jsonWriter, cmd);
                }
                catch (Exception e)
                {
                    var error = await GetExceptionFromOperation().ConfigureAwait(false);
                    if (error != null)
                    {
                        throw error;
                    }
                    await ThrowOnUnavailableStream(id,e).ConfigureAwait(false);
                }
            }
        }

        private async Task<BulkInsertAbortedException> GetExceptionFromOperation()
        {
            var stateRequest = new GetOperationStateCommand(_conventions,_operationId);
            await _requestExecuter.ExecuteAsync(stateRequest, _context, _token).ConfigureAwait(false);
            var error = stateRequest.Result.Result as OperationExceptionResult;
           
            if (error == null)
                return null;
            return new BulkInsertAbortedException(error.Error);
        }

        private async Task EnsureStream()
        {
            var bulkCommand = new BulkInsertCommand(_operationId,_streamExposerContent);
            _bulkInsertExecuteTask = _requestExecuter.ExecuteAsync(bulkCommand, _context, _token);

            _stream = await _streamExposerContent.OutputStream.ConfigureAwait(false);
            _jsonWriter = new BlittableJsonTextWriter(_context, _stream);
            _jsonWriter.WriteStartArray();
        }

        private async Task ThrowOnUnavailableStream(string id, Exception innerEx)
        {
            _streamExposerContent.ErrorOnProcessingRequest(
                new BulkInsertAbortedException($"Write to stream failed at document with id {id}." + Environment.NewLine +
                                               $"This does not mean, that the server put this document.", innerEx));           
            await _bulkInsertExecuteTask.ConfigureAwait(false);                       
        }

        public async Task AsyncKill()
        {
            if (_operationId == -1)
                return; // nothing was done, nothing to kill
            await WaitForId().ConfigureAwait(false);
            try
            {
                await _requestExecuter.ExecuteAsync(new KillOperationCommand(_operationId), _context, _token).ConfigureAwait(false);
            }
            catch (RavenException)
            {
                throw new BulkInsertAbortedException("Unable to kill this bulk insert operation, because it was not found on the server.");
            }
        }

        public void Kill()
        {
            AsyncHelpers.RunSync(AsyncKill);
        }

        public void Dispose()
        {
            AsyncHelpers.RunSync(DisposeAsync);
        }

        public async Task DisposeAsync()
        {
            try
            {
                if (_stream != null)
                {
                    _jsonWriter.WriteEndArray();
                    _jsonWriter.Flush();
                    await _stream.FlushAsync(_token).ConfigureAwait(false);
                }

                _streamExposerContent.Done();

                if (_operationId == -1)
                {
                    // closing without calling a single store. 
                    return;
                }

                if (_bulkInsertExecuteTask != null)
                {
                    try
                    {
                        await _bulkInsertExecuteTask.ConfigureAwait(false);
                    }
                    catch
                    {
                        var error = await GetExceptionFromOperation().ConfigureAwait(false);
                        if (error != null)
                        {
                            throw error;
                        }
                        throw;
                    }                                 
                }                 
            }
            finally
            {
                _streamExposerContent?.Dispose();
                _resetContext.Dispose();
            }
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
