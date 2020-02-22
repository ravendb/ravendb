using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Identity;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.BulkInsert;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Threading;

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
                    throw new BulkInsertProtocolViolationException("Unable to close the stream", _done.Task.Exception);
                }
            }

            protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
            {
                _outputStreamTcs.TrySetResult(stream);

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

            public BulkInsertCommand(long id, StreamExposerContent stream)
            {
                _stream = stream;
                _id = id;
                Timeout = TimeSpan.FromHours(12); // global max timeout
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/bulk_insert?id={_id}";
                var message = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = _stream,
                    Headers =
                    {
                        TransferEncodingChunked = true
                    }
                };

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

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                throw new NotImplementedException();
            }
        }

        private readonly RequestExecutor _requestExecutor;
        private Task _bulkInsertExecuteTask;

        private readonly JsonOperationContext _context;
        private readonly IDisposable _resetContext;

        private Stream _stream;
        private readonly StreamExposerContent _streamExposerContent;
        private bool _first = true;
        private CommandType _previousCommandType = CommandType.None;
        private string _previousId = null;
        private int _numberOfTimeSeriesForId = 0;
        private const int _maxNumberOfTimeSeriesForIdInBatch = 10 * 1024;
        private long _operationId = -1;

        public CompressionLevel CompressionLevel = CompressionLevel.NoCompression;
        private readonly JsonSerializer _defaultSerializer;
        private readonly Func<object, IMetadataDictionary, StreamWriter, bool> _customEntitySerializer;
        private long _concurrentCheck;

        public BulkInsertOperation(string database, IDocumentStore store, CancellationToken token = default)
        {
            _disposeOnce = new DisposeOnceAsync<SingleAttempt>(async () =>
            {
                try
                {
                    Exception flushEx = null;

                    if (_stream != null)
                    {
                        try
                        {
                            EndPreviousCommandIfNeeded();

                            _currentWriter.Write(']');
                            _currentWriter.Flush();
                            await _asyncWrite.ConfigureAwait(false);
                            ((MemoryStream)_currentWriter.BaseStream).TryGetBuffer(out var buffer);
                            await _requestBodyStream.WriteAsync(buffer.Array, buffer.Offset, buffer.Count, _token).ConfigureAwait(false);
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
                            await ThrowBulkInsertAborted(e, flushEx).ConfigureAwait(false);
                        }
                    }
                }
                finally
                {
                    _streamExposerContent?.Dispose();
                    _resetContext.Dispose();
                }
            });

            _token = token;
            _conventions = store.Conventions;
            _requestExecutor = store.GetRequestExecutor(database);
            _resetContext = _requestExecutor.ContextPool.AllocateOperationContext(out _context);
            _currentWriter = new StreamWriter(new MemoryStream());
            _backgroundWriter = new StreamWriter(new MemoryStream());
            _streamExposerContent = new StreamExposerContent();

            _defaultSerializer = _requestExecutor.Conventions.CreateSerializer();
            _customEntitySerializer = _requestExecutor.Conventions.BulkInsert.TrySerializeEntityToJsonStream;

            _generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(_requestExecutor.Conventions,
                entity => AsyncHelpers.RunSync(() => _requestExecutor.Conventions.GenerateDocumentIdAsync(database, entity)));
        }

        private async Task ThrowBulkInsertAborted(Exception e, Exception flushEx = null)
        {
            var errors = new List<Exception>(3);

            var error = await GetExceptionFromOperation().ConfigureAwait(false);

            if (error != null)
                errors.Add(error);

            if (flushEx != null)
                errors.Add(flushEx);

            errors.Add(e);

            throw new BulkInsertAbortedException("Failed to execute bulk insert", new AggregateException(errors));
        }

        private async Task WaitForId()
        {
            if (_operationId != -1)
                return;

            var bulkInsertGetIdRequest = new GetNextOperationIdCommand();
            await _requestExecutor.ExecuteAsync(bulkInsertGetIdRequest, _context, sessionInfo: null, token: _token).ConfigureAwait(false);
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
                id = GetId(entity);

            await StoreAsync(entity, id, metadata).ConfigureAwait(false);

            return id;
        }

        public async Task StoreAsync(object entity, string id, IMetadataDictionary metadata = null)
        {
            using (ConcurrencyCheck())
            {
                VerifyValidId(id);

                await ExecuteBeforeStore().ConfigureAwait(false);

                if (metadata == null)
                    metadata = new MetadataAsDictionary();

                if (metadata.ContainsKey(Constants.Documents.Metadata.Collection) == false)
                {
                    var collection = _requestExecutor.Conventions.GetCollectionName(entity);
                    if (collection != null)
                        metadata.Add(Constants.Documents.Metadata.Collection, collection);
                }

                if (metadata.ContainsKey(Constants.Documents.Metadata.RavenClrType) == false)
                {
                    var clrType = _requestExecutor.Conventions.GetClrTypeName(entity.GetType());
                    if (clrType != null)
                        metadata[Constants.Documents.Metadata.RavenClrType] = clrType;
                }

                await WriteToStream(writeComma: true, () =>
                {
                    _currentWriter.Write("{\"Id\":\"");
                    WriteString(_currentWriter, id);
                    _currentWriter.Write("\",\"Type\":\"PUT\",\"Document\":");

                    if (_customEntitySerializer == null || _customEntitySerializer(entity, metadata, _currentWriter) == false)
                    {
                        using (var json = EntityToBlittable.ConvertEntityToBlittable(entity, _conventions, _context,
                            _defaultSerializer, new DocumentInfo {MetadataInstance = metadata}))
                        {
                            _currentWriter.Flush();
                            json.WriteJsonTo(_currentWriter.BaseStream);
                        }
                    }

                    _currentWriter.Write("}");
                }, id, CommandType.PUT).ConfigureAwait(false);
            }
        }

        public void AppendTimeSeries(string id, string timeseries, DateTime timestamp, string tag, IEnumerable<double> values)
        {
            AsyncHelpers.RunSync(() => AppendTimeSeriesAsync(id, timeseries, timestamp, tag, values));
        }

        public async Task AppendTimeSeriesAsync(string id, string timeseries, DateTime timestamp, string tag, IEnumerable<double> values)
        {
            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentNullException(nameof(id));

            if (string.IsNullOrWhiteSpace(timeseries))
                throw new ArgumentNullException(nameof(timeseries));

            using (ConcurrencyCheck())
            {
                await ExecuteBeforeStore().ConfigureAwait(false);

                var isNewTimeSeries = _previousCommandType != CommandType.TimeSeries ||
                                   _previousId.Equals(id, StringComparison.OrdinalIgnoreCase) == false;

                if (isNewTimeSeries == false && _numberOfTimeSeriesForId > _maxNumberOfTimeSeriesForIdInBatch)
                {
                    isNewTimeSeries = true;
                    _numberOfTimeSeriesForId = 0;
                }

                await WriteToStream(writeComma: isNewTimeSeries, () =>
                {
                    if (isNewTimeSeries)
                    {
                        // writing a new time series for a new document
                        _currentWriter.Write("{\"Id\":\"");
                        WriteString(_currentWriter, id);
                        _currentWriter.Write("\",\"Type\":\"TimeSeries\",\"TimeSeries\":{\"DocumentId\":\"");
                        WriteString(_currentWriter, id);
                        _currentWriter.Write("\",\"Appends\":[");
                    }
                    else
                    {
                        // this is a time series append for the same document id
                        _currentWriter.Write(',');
                        _numberOfTimeSeriesForId++;
                    }

                    var appendOperation = new TimeSeriesOperation.AppendOperation
                    {
                        Name = timeseries,
                        Timestamp = timestamp,
                        Tag = tag,
                        Values = values is double[] arr
                            ? arr
                            : values.ToArray()
                    };

                    using (var json = _context.ReadObject(appendOperation.ToJson(), id))
                    {
                        _currentWriter.Flush();
                        json.WriteJsonTo(_currentWriter.BaseStream);
                    }
                }, id, CommandType.TimeSeries).ConfigureAwait(false);
            }
        }

        private async Task WriteToStream(bool writeComma, Action writeOperation, string documentId, CommandType commandType)
        {
            if (writeComma)
            {
                EndPreviousCommandIfNeeded();

                if (_first == false)
                {
                    _currentWriter.Write(',');
                }

                _first = false;
            }

            try
            {
                writeOperation();

                _currentWriter.Flush();

                if (_currentWriter.BaseStream.Position > _maxSizeInBuffer ||
                    _asyncWrite.IsCompleted)
                {
                    await _asyncWrite.ConfigureAwait(false);

                    var tmp = _currentWriter;
                    _currentWriter = _backgroundWriter;
                    _backgroundWriter = tmp;
                    _currentWriter.BaseStream.SetLength(0);
                    ((MemoryStream)tmp.BaseStream).TryGetBuffer(out var buffer);
                    _asyncWrite = _requestBodyStream.WriteAsync(buffer.Array, buffer.Offset, buffer.Count, _token);
                }

                _previousId = documentId;
                _previousCommandType = commandType;
            }
            catch (Exception e)
            {
                var error = await GetExceptionFromOperation().ConfigureAwait(false);
                if (error != null)
                {
                    throw error;
                }

                await ThrowOnUnavailableStream(documentId, e).ConfigureAwait(false);
            }
        }

        private void EndPreviousCommandIfNeeded()
        {
            if (_previousCommandType == CommandType.TimeSeries)
                _currentWriter.Write("]}}");
        }

        private IDisposable ConcurrencyCheck()
        {
            if (Interlocked.CompareExchange(ref _concurrentCheck, 1, 0) == 1)
                throw new InvalidOperationException("Bulk Insert store methods cannot be executed concurrently.");

            return new DisposableAction(() => Interlocked.CompareExchange(ref _concurrentCheck, 0, 1));
        }

        private static void WriteString(StreamWriter writer, string input)
        {
            for (var i = 0; i < input.Length; i++)
            {
                var c = input[i];
                if (c == '"')
                {
                    if (i == 0 || input[i - 1] != '\\')
                        writer.Write("\\");
                }

                writer.Write(c);
            }
        }

        private async Task ExecuteBeforeStore()
        {
            if (_stream == null)
            {
                await WaitForId().ConfigureAwait(false);
                await EnsureStream().ConfigureAwait(false);
            }

            if (_bulkInsertExecuteTask.IsFaulted)
            {
                try
                {
                    await _bulkInsertExecuteTask.ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    await ThrowBulkInsertAborted(e).ConfigureAwait(false);
                }
            }
        }

        private static void VerifyValidId(string id)
        {
            if (string.IsNullOrEmpty(id))
            {
                throw new InvalidOperationException("Document id must have a non empty value");
            }

            if (id.EndsWith("|"))
            {
                throw new NotSupportedException("Document ids cannot end with '|', but was called with " + id);
            }
        }

        private async Task<BulkInsertAbortedException> GetExceptionFromOperation()
        {
            var stateRequest = new GetOperationStateOperation.GetOperationStateCommand(_requestExecutor.Conventions, _operationId);
            await _requestExecutor.ExecuteAsync(stateRequest, _context, sessionInfo: null, token: _token).ConfigureAwait(false);

            if (!(stateRequest.Result?.Result is OperationExceptionResult error))
                return null;
            return new BulkInsertAbortedException(error.Error);
        }

        private GZipStream _compressedStream;
        private Stream _requestBodyStream;
        private StreamWriter _currentWriter;
        private StreamWriter _backgroundWriter;
        private Task _asyncWrite = Task.CompletedTask;
        private int _maxSizeInBuffer = 1024 * 1024;


        private async Task EnsureStream()
        {
            if (CompressionLevel != CompressionLevel.NoCompression)
                _streamExposerContent.Headers.ContentEncoding.Add("gzip");

            var bulkCommand = new BulkInsertCommand(
                _operationId,
                _streamExposerContent);
            _bulkInsertExecuteTask = _requestExecutor.ExecuteAsync(bulkCommand, _context, sessionInfo: null, token: _token);

            _stream = await _streamExposerContent.OutputStream.ConfigureAwait(false);

            _requestBodyStream = _stream;
            if (CompressionLevel != CompressionLevel.NoCompression)
            {
                _compressedStream = new GZipStream(_stream, CompressionLevel, leaveOpen: true);
                _requestBodyStream = _compressedStream;
            }

            _currentWriter.Write('[');
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
                await _requestExecutor.ExecuteAsync(new KillOperationCommand(_operationId), _context, sessionInfo: null, token: _token).ConfigureAwait(false);
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

        /// <summary>
        /// The problem with this function is that it could be run but not
        /// awaited. If this happens, then we could concurrently dispose the
        /// bulk insert operation from two different threads. This is the
        /// reason we are using a dispose once.
        /// </summary>
        private readonly DisposeOnceAsync<SingleAttempt> _disposeOnce;

        private readonly DocumentConventions _conventions;

        public Task DisposeAsync()
        {
            return _disposeOnce.DisposeAsync();
        }

        private string GetId(object entity)
        {
            if (_generateEntityIdOnTheClient.TryGetIdFromInstance(entity, out var id))
                return id;

            id = _generateEntityIdOnTheClient.GenerateDocumentIdForStorage(entity);
            _generateEntityIdOnTheClient.TrySetIdentity(entity, id); //set Id property if it was null
            return id;
        }
    }
}
