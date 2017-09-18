using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Identity;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.BulkInsert;
using Raven.Client.Extensions;
using Raven.Client.Http;
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
                _outputStreamTcs = new TaskCompletionSource<Stream>(TaskCreationOptions.RunContinuationsAsynchronously);
                OutputStream = _outputStreamTcs.Task;
                _done = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
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
                // run the completion asynchronously to ensure that continuations (await WaitAsync()) won't happen as part of a call to TrySetResult
                // http://blogs.msdn.com/b/pfxteam/archive/2012/02/11/10266920.aspx

                var currentTcs = _outputStreamTcs;

                Task.Factory.StartNew(s => ((TaskCompletionSource<Stream>)s).TrySetResult(stream),
                    currentTcs, CancellationToken.None,
                    TaskCreationOptions.PreferFairness | TaskCreationOptions.RunContinuationsAsynchronously,
                    TaskScheduler.Default);

                currentTcs.Task.Wait();

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

            protected override void Dispose(bool disposing)
            {
                _done.TrySetCanceled();

                //after dispose we don't care for unobserved exceptions
                _done.Task.IgnoreUnobservedExceptions();
                _outputStreamTcs.Task.IgnoreUnobservedExceptions();
            }
        }

        private class BulkInsertCommand : RavenCommand<HttpResponseMessage>
        {
            public override bool IsReadRequest => false;
            private readonly StreamExposerContent _stream;
            private readonly long _id;
            private readonly bool _useGzipCompression;

            public BulkInsertCommand(long id, StreamExposerContent stream, bool useGzipCompression)
            {
                _stream = stream;
                _id = id;
                Timeout = TimeSpan.FromHours(12); // global max timeout
                _useGzipCompression = useGzipCompression;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/bulk_insert?id={_id}";
                var message = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = _stream
                };

                if (_useGzipCompression)
                    message.Headers.Add(Constants.Headers.ContentEncoding, new[] { "gzip" });

                return message;
            }

            public override async Task<HttpResponseMessage> SendAsync(HttpClient client, HttpRequestMessage request, CancellationToken token)
            {
                try
                {
                    return await base.SendAsync(client, request, token).ConfigureAwait(false);
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
        private readonly RequestExecutor _requestExecutor;
        private Task _bulkInsertExecuteTask;

        private readonly JsonOperationContext _context;
        private readonly IDisposable _resetContext;

        private BlittableJsonTextWriter _jsonWriter;
        private Stream _stream;
        private readonly StreamExposerContent _streamExposerContent;

        private bool _first = true;
        private long _operationId = -1;

        public CompressionLevel UseGzipCompressionLevel = CompressionLevel.Fastest;

        public BulkInsertOperation(string database, IDocumentStore store, CancellationToken token = default(CancellationToken))
        {
            _token = token;
            _requestExecutor = store.GetRequestExecutor(database);
            _resetContext = _requestExecutor.ContextPool.AllocateOperationContext(out _context);
            _streamExposerContent = new StreamExposerContent();

            _generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(_requestExecutor.Conventions, entity => AsyncHelpers.RunSync(() => _requestExecutor.Conventions.GenerateDocumentIdAsync(database, entity)));
        }

        private async Task WaitForId()
        {
            if (_operationId != -1)
                return;

            var bulkInsertGetIdRequest = new GetNextOperationIdCommand();
            await _requestExecutor.ExecuteAsync(bulkInsertGetIdRequest, _context, _token).ConfigureAwait(false);
            _operationId = bulkInsertGetIdRequest.Result;
        }

        public void Store(object entity, string id, IMetadataDictionary metadata = null)
        {
            AsyncHelpers.RunSync(() => StoreAsync(entity, id, metadata));
        }

        public string Store(object entity, IMetadataDictionary metadata = null)
        {
            return AsyncHelpers.RunSync(() => StoreAsync(entity, metadata));
        }

        public async Task<string> StoreAsync(object entity, IMetadataDictionary metadata = null)
        {
            if (metadata == null || metadata.TryGetValue(Constants.Documents.Metadata.Id, out var id) == false)
            {
                id = GetId(entity);
            }

            await StoreAsync(entity, id , metadata).ConfigureAwait(false);
            return id;
        }

        public async Task StoreAsync(object entity, string id , IMetadataDictionary metadata = null)
        {
            VerifyValidId(id);

            if (_stream == null)
            {
                await WaitForId().ConfigureAwait(false);
                await EnsureStream().ConfigureAwait(false);
            }

            var docInfo = new DocumentInfo
            {
                Collection = _requestExecutor.Conventions.GetCollectionName(entity)
            };

            JsonOperationContext tempContext;
            using (_requestExecutor.ContextPool.AllocateOperationContext(out tempContext))
            {
                if (metadata != null)
                {
                    docInfo.MetadataInstance = metadata;
                    docInfo.Metadata = EntityToBlittable.ConvertEntityToBlittable(metadata, _requestExecutor.Conventions, tempContext);
                }

                using (var doc = EntityToBlittable.ConvertEntityToBlittable(entity, _requestExecutor.Conventions, tempContext, docInfo))
                {
                    if (_first == false)
                    {
                        _jsonWriter.WriteComma();
                    }
                    _first = false;

                    var cmd = new DynamicJsonValue
                    {
                        [nameof(PutCommandDataWithBlittableJson.Type)] = "PUT",
                        [nameof(PutCommandDataWithBlittableJson.Id)] = id,
                        [nameof(PutCommandDataWithBlittableJson.Document)] = doc
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
                        await ThrowOnUnavailableStream(id, e).ConfigureAwait(false);
                    }
                } 
            }
        }

        private static void VerifyValidId(string id)
        {
            if (string.IsNullOrEmpty(id))
            {
                throw new InvalidOperationException("Document id must have a non empty value");
            }

            if (id.EndsWith("/"))
            {
                throw new NotSupportedException("Document ids cannot end with '/', but was called with " + id);
            }
        }

        private async Task<BulkInsertAbortedException> GetExceptionFromOperation()
        {
            var stateRequest = new GetOperationStateCommand(_requestExecutor.Conventions, _operationId);
            await _requestExecutor.ExecuteAsync(stateRequest, _context, _token).ConfigureAwait(false);
            var error = stateRequest.Result.Result as OperationExceptionResult;

            if (error == null)
                return null;
            return new BulkInsertAbortedException(error.Error);
        }


        private GZipStream _compressedStream = null;

        private async Task EnsureStream()
        {
            var bulkCommand = new BulkInsertCommand(
                _operationId,
                _streamExposerContent,
                UseGzipCompressionLevel != CompressionLevel.NoCompression);
            _bulkInsertExecuteTask = _requestExecutor.ExecuteAsync(bulkCommand, _context, _token);

            _stream = await _streamExposerContent.OutputStream.ConfigureAwait(false);

            var requestBodyStream = _stream;
            if (UseGzipCompressionLevel != CompressionLevel.NoCompression)
            {
                _compressedStream = new GZipStream(_stream, UseGzipCompressionLevel, leaveOpen: true);
                requestBodyStream = _compressedStream;
            }

            _jsonWriter = new BlittableJsonTextWriter(_context, requestBodyStream);
            _jsonWriter.WriteStartArray();
        }

        private async Task ThrowOnUnavailableStream(string id, Exception innerEx)
        {
            _streamExposerContent.ErrorOnProcessingRequest(
                new BulkInsertAbortedException($"Write to stream failed at document with id {id}.", innerEx));
            await _bulkInsertExecuteTask.ConfigureAwait(false);
        }

        public async Task AbortAsync()
        {
            if (_operationId == -1)
                return; // nothing was done, nothing to kill
            await WaitForId().ConfigureAwait(false);
            try
            {
                await _requestExecutor.ExecuteAsync(new KillOperationCommand(_operationId), _context, _token).ConfigureAwait(false);
            }
            catch (RavenException)
            {
                throw new BulkInsertAbortedException("Unable to kill this bulk insert operation, because it was not found on the server.");
            }
        }

        public void Abort()
        {
            AsyncHelpers.RunSync(AbortAsync);
        }

        public void Dispose()
        {
            AsyncHelpers.RunSync(DisposeAsync);
        }

        public async Task DisposeAsync()
        {
            try
            {
                Exception flushEx = null;

                if (_stream != null)
                {
                    try
                    {
                        _jsonWriter.WriteEndArray();
                        _jsonWriter.Flush();
                        _compressedStream?.Dispose();
                        await _stream.FlushAsync(_token).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        flushEx = e;
                    }
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
                    catch (Exception e)
                    {
                        var errors = new List<Exception>(3) { e };
                        if (flushEx != null)
                            errors.Add(flushEx);
                        var error = await GetExceptionFromOperation().ConfigureAwait(false);
                        if (error != null)
                        {
                            errors.Add(error);
                        }
                        errors.Reverse();
                        throw new BulkInsertAbortedException("Failed to execute bulk insert", new AggregateException(errors));

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
                id = _generateEntityIdOnTheClient.GenerateDocumentIdForStorage(entity);
                _generateEntityIdOnTheClient.TrySetIdentity(entity, id); //set Id property if it was null
            }
            return id;
        }
    }
}
