using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Identity;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Documents.TimeSeries;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.BulkInsert;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Json;
using Sparrow.Threading;

namespace Raven.Client.Documents.BulkInsert
{
    public class BulkInsertOperation : IDisposable, IAsyncDisposable
    {
        private readonly CancellationToken _token;
        private readonly GenerateEntityIdOnTheClient _generateEntityIdOnTheClient;

        private class StreamExposerContent : HttpContent
        {
            public readonly Task<Stream> OutputStream;
            private readonly TaskCompletionSource<Stream> _outputStreamTcs;
            private readonly TaskCompletionSource<object> _done;

            public bool IsDone => _done.Task.IsCompleted;

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

            public BulkInsertCommand(long id, StreamExposerContent stream, string nodeTag)
            {
                _stream = stream;
                _id = id;
                SelectedNodeTag = nodeTag;
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
        private CommandType _inProgressCommand;
        private readonly CountersBulkInsertOperation _countersOperation;
        private readonly AttachmentsBulkInsertOperation _attachmentsOperation;
        private long _operationId = -1;
        private string _nodeTag;

        public CompressionLevel CompressionLevel = CompressionLevel.NoCompression;
        private readonly IJsonSerializer _defaultSerializer;
        private readonly Func<object, IMetadataDictionary, StreamWriter, bool> _customEntitySerializer;
        private readonly int _timeSeriesBatchSize;
        private long _concurrentCheck;

        public BulkInsertOperation(string database, IDocumentStore store, CancellationToken token = default)
        {
            _disposeOnce = new DisposeOnceAsync<SingleAttempt>(async () =>
            {
                try
                {
                    if (_streamExposerContent.IsDone)
                        return;

                    EndPreviousCommandIfNeeded();

                    Exception flushEx = null;

                    if (_stream != null)
                    {
                        try
                        {
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
            if (string.IsNullOrWhiteSpace(database))
                ThrowNoDatabase();
            _requestExecutor = store.GetRequestExecutor(database);
            _resetContext = _requestExecutor.ContextPool.AllocateOperationContext(out _context);
            _currentWriter = new StreamWriter(new MemoryStream());
            _backgroundWriter = new StreamWriter(new MemoryStream());
            _streamExposerContent = new StreamExposerContent();
            _countersOperation = new CountersBulkInsertOperation(this);
            _attachmentsOperation = new AttachmentsBulkInsertOperation(this);

            _defaultSerializer = _requestExecutor.Conventions.Serialization.CreateSerializer();
            _customEntitySerializer = _requestExecutor.Conventions.BulkInsert.TrySerializeEntityToJsonStream;
            _timeSeriesBatchSize = _conventions.BulkInsert.TimeSeriesBatchSize;

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

        private void ThrowNoDatabase()
        {
            throw new InvalidOperationException(
                $"Cannot start bulk insert operation without specifying a name of a database to operate on. " +
                $"Database name can be passed as an argument when bulk insert is being created or default database can be defined using '{nameof(DocumentStore)}.{nameof(IDocumentStore.Database)}' property.");
        }

        private async Task WaitForId()
        {
            if (_operationId != -1)
                return;

            var bulkInsertGetIdRequest = new GetNextOperationIdCommand();
            await ExecuteAsync(bulkInsertGetIdRequest, token: _token).ConfigureAwait(false);
            _operationId = bulkInsertGetIdRequest.Result;
            _nodeTag = bulkInsertGetIdRequest.NodeTag;
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
                    var clrType = _requestExecutor.Conventions.GetClrTypeName(entity);
                    if (clrType != null)
                        metadata[Constants.Documents.Metadata.RavenClrType] = clrType;
                }

                EndPreviousCommandIfNeeded();

                try
                {
                    if (_first == false)
                    {
                        WriteComma();
                    }

                    _first = false;
                    _inProgressCommand = CommandType.None;

                    _currentWriter.Write("{\"Id\":\"");
                    WriteString(id);
                    _currentWriter.Write("\",\"Type\":\"PUT\",\"Document\":");

                    await FlushIfNeeded().ConfigureAwait(false);

                    if (_customEntitySerializer == null || _customEntitySerializer(entity, metadata, _currentWriter) == false)
                    {
                        using (var json = _conventions.Serialization.DefaultConverter.ToBlittable(entity, metadata, _context, _defaultSerializer))
                            await json.WriteJsonToAsync(_currentWriter.BaseStream, _token).ConfigureAwait(false);
                    }

                    _currentWriter.Write('}');
                }
                catch (Exception e)
                {
                    await HandleErrors(id, e).ConfigureAwait(false);
                }
            }
        }

        private async Task HandleErrors(string documentId, Exception e)
        {
            BulkInsertAbortedException errorFromServer = null;
            try
            {
                errorFromServer = await GetExceptionFromOperation().ConfigureAwait(false);
            }
            catch
            {
                // server is probably down, will propagate the original exception
            }

            if (errorFromServer != null)
                throw errorFromServer;

            await ThrowOnUnavailableStream(documentId, e).ConfigureAwait(false);
        }

        private IDisposable ConcurrencyCheck()
        {
            if (Interlocked.CompareExchange(ref _concurrentCheck, 1, 0) == 1)
                throw new InvalidOperationException("Bulk Insert store methods cannot be executed concurrently.");

            return new DisposableAction(() => Interlocked.CompareExchange(ref _concurrentCheck, 0, 1));
        }

        private async Task FlushIfNeeded()
        {
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
        }

        private void EndPreviousCommandIfNeeded()
        {
            if (_inProgressCommand == CommandType.Counters)
            {
                _countersOperation.EndPreviousCommandIfNeeded();
            }
            else if (_inProgressCommand == CommandType.TimeSeries)
            {
                TimeSeriesBulkInsert.ThrowAlreadyRunningTimeSeries();
            }
        }

        private void WriteString(string input)
        {
            for (var i = 0; i < input.Length; i++)
            {
                var c = input[i];
                if (c == '"')
                {
                    if (i == 0 || input[i - 1] != '\\')
                        _currentWriter.Write('\\');
                }

                _currentWriter.Write(c);
            }
        }

        private void WriteComma()
        {
            _currentWriter.Write(',');
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
            var stateRequest = new GetOperationStateOperation.GetOperationStateCommand(_operationId, _nodeTag);
            await ExecuteAsync(stateRequest, token: _token).ConfigureAwait(false);

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
                _streamExposerContent,
                _nodeTag);

            _bulkInsertExecuteTask = ExecuteAsync(bulkCommand);

            _stream = await _streamExposerContent.OutputStream.ConfigureAwait(false);

            _requestBodyStream = _stream;
            if (CompressionLevel != CompressionLevel.NoCompression)
            {
                _compressedStream = new GZipStream(_stream, CompressionLevel, leaveOpen: true);
                _requestBodyStream = _compressedStream;
            }

            _currentWriter.Write('[');
        }

        private async Task ExecuteAsync(BulkInsertCommand cmd)
        {
            try
            {
                await ExecuteAsync(cmd, token: _token).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _streamExposerContent.ErrorOnRequestStart(e);
                throw;
            }
        }

        private async Task ExecuteAsync<TResult>(
            RavenCommand<TResult> command,
            CancellationToken token = default)
        {
            using (_requestExecutor.ContextPool.AllocateOperationContext(out var context))
            {
                await _requestExecutor.ExecuteAsync(command, context, token: token).ConfigureAwait(false);
            }
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
                await ExecuteAsync(new KillOperationCommand(_operationId, _nodeTag), token: _token).ConfigureAwait(false);
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
            AsyncHelpers.RunSync(() => DisposeAsync().AsTask());
        }

        /// <summary>
        /// The problem with this function is that it could be run but not
        /// awaited. If this happens, then we could concurrently dispose the
        /// bulk insert operation from two different threads. This is the
        /// reason we are using a dispose once.
        /// </summary>
        private readonly DisposeOnceAsync<SingleAttempt> _disposeOnce;

        private readonly DocumentConventions _conventions;

        public async ValueTask DisposeAsync()
        {
            await _disposeOnce.DisposeAsync().ConfigureAwait(false);
        }

        private string GetId(object entity)
        {
            if (_generateEntityIdOnTheClient.TryGetIdFromInstance(entity, out var id))
                return id;

            id = _generateEntityIdOnTheClient.GenerateDocumentIdForStorage(entity);
            _generateEntityIdOnTheClient.TrySetIdentity(entity, id); //set Id property if it was null
            return id;
        }

        public AttachmentsBulkInsert AttachmentsFor(string id)
        {
            if (string.IsNullOrEmpty(id))
                throw new ArgumentException("Document id cannot be null or empty", nameof(id));

            return new AttachmentsBulkInsert(this, id);
        }

        public CountersBulkInsert CountersFor(string id)
        {
            if (string.IsNullOrEmpty(id))
                throw new ArgumentException("Document id cannot be null or empty", nameof(id));

            return new CountersBulkInsert(this, id);
        }

        public TimeSeriesBulkInsert TimeSeriesFor(string id, string name)
        {
            if (string.IsNullOrEmpty(id))
                throw new ArgumentException("Document id cannot be null or empty", nameof(id));

            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("Time series name cannot be null or empty", nameof(name));

            return new TimeSeriesBulkInsert(this, id, name);
        }

        public TypedTimeSeriesBulkInsert<TValues> TimeSeriesFor<TValues>(string id, string name = null) where TValues : new()
        {
            if (string.IsNullOrEmpty(id))
                throw new ArgumentException("Document id cannot be null or empty", nameof(id));

            var tsName = name ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(_conventions);
            if (string.IsNullOrEmpty(tsName))
                throw new ArgumentException("Time series name cannot be null or empty", nameof(name));

            return new TypedTimeSeriesBulkInsert<TValues>(this, id, tsName);
        }

        public struct CountersBulkInsert
        {
            private readonly BulkInsertOperation _operation;
            private readonly string _id;

            public CountersBulkInsert(BulkInsertOperation operation, string id)
            {
                _operation = operation;
                _id = id;
            }

            public void Increment(string name, long delta = 1L)
            {
                _operation._countersOperation.Increment(_id, name, delta);
            }

            public Task IncrementAsync(string name, long delta = 1L)
            {
                return _operation._countersOperation.IncrementAsync(_id, name, delta);
            }
        }

        internal class CountersBulkInsertOperation
        {
            private readonly BulkInsertOperation _operation;
            private string _id;
            private bool _first = true;
            private const int _maxCountersInBatch = 1024;
            private int _countersInBatch = 0;

            public CountersBulkInsertOperation(BulkInsertOperation bulkInsertOperation)
            {
                _operation = bulkInsertOperation;
            }

            public void Increment(string id, string name, long delta = 1L)
            {
                AsyncHelpers.RunSync(() => IncrementAsync(id, name, delta));
            }

            public async Task IncrementAsync(string id, string name, long delta)
            {
                using (_operation.ConcurrencyCheck())
                {
                    await _operation.ExecuteBeforeStore().ConfigureAwait(false);

                    if (_operation._inProgressCommand == CommandType.TimeSeries)
                        TimeSeriesBulkInsert.ThrowAlreadyRunningTimeSeries();

                    try
                    {
                        var isFirst = _id == null;
                        if (isFirst || _id.Equals(id, StringComparison.OrdinalIgnoreCase) == false)
                        {
                            if (isFirst == false)
                            {
                                //we need to end the command for the previous document id
                                _operation._currentWriter.Write("]}},");
                            }
                            else if (_operation._first == false)
                            {
                                _operation.WriteComma();
                            }

                            _operation._first = false;

                            _id = id;
                            _operation._inProgressCommand = CommandType.Counters;

                            WritePrefixForNewCommand();
                        }

                        if (_countersInBatch >= _maxCountersInBatch)
                        {
                            _operation._currentWriter.Write("]}},");

                            WritePrefixForNewCommand();
                        }

                        _countersInBatch++;

                        if (_first == false)
                        {
                            _operation.WriteComma();
                        }

                        _first = false;

                        _operation._currentWriter.Write("{\"Type\":\"Increment\",\"CounterName\":\"");
                        _operation.WriteString(name);
                        _operation._currentWriter.Write("\",\"Delta\":");
                        _operation._currentWriter.Write(delta);
                        _operation._currentWriter.Write('}');

                        await _operation.FlushIfNeeded().ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        await _operation.HandleErrors(_id, e).ConfigureAwait(false);
                    }
                }
            }

            public void EndPreviousCommandIfNeeded()
            {
                if (_id == null)
                    return;

                _operation._currentWriter.Write("]}}");
                _id = null;
            }

            private void WritePrefixForNewCommand()
            {
                _first = true;
                _countersInBatch = 0;

                _operation._currentWriter.Write("{\"Id\":\"");
                _operation.WriteString(_id);
                _operation._currentWriter.Write("\",\"Type\":\"Counters\",\"Counters\":{\"DocumentId\":\"");
                _operation.WriteString(_id);
                _operation._currentWriter.Write("\",\"Operations\":[");
            }
        }

        public abstract class TimeSeriesBulkInsertBase : IDisposable
        {
            private readonly BulkInsertOperation _operation;
            private readonly string _id;
            private readonly string _name;
            private bool _first = true;
            private int _timeSeriesInBatch = 0;

            protected TimeSeriesBulkInsertBase(BulkInsertOperation operation, string id, string name)
            {
                operation.EndPreviousCommandIfNeeded();

                _operation = operation;
                _id = id;
                _name = name;

                _operation._inProgressCommand = CommandType.TimeSeries;
            }

            protected async Task AppendAsyncInternal(DateTime timestamp, ICollection<double> values, string tag = null)
            {
                using (_operation.ConcurrencyCheck())
                {
                    await _operation.ExecuteBeforeStore().ConfigureAwait(false);

                    try
                    {
                        if (_first)
                        {
                            if (_operation._first == false)
                                _operation.WriteComma();

                            WritePrefixForNewCommand();
                        }
                        else if (_timeSeriesInBatch >= _operation._timeSeriesBatchSize)
                        {
                            _operation._currentWriter.Write("]}},");
                            WritePrefixForNewCommand();
                        }

                        _timeSeriesInBatch++;

                        if (_first == false)
                        {
                            _operation.WriteComma();
                        }

                        _first = false;

                        _operation._currentWriter.Write('[');

                        timestamp = timestamp.EnsureUtc();
                        _operation._currentWriter.Write(timestamp.Ticks);
                        _operation.WriteComma();

                        _operation._currentWriter.Write(values.Count);
                        _operation.WriteComma();

                        var firstValue = true;
                        foreach (var value in values)
                        {
                            if (firstValue == false)
                                _operation.WriteComma();

                            firstValue = false;
                            _operation._currentWriter.Write(value.ToString("R", CultureInfo.InvariantCulture));
                        }

                        if (tag != null)
                        {
                            _operation._currentWriter.Write(",\"");
                            _operation.WriteString(tag);
                            _operation._currentWriter.Write('\"');
                        }

                        _operation._currentWriter.Write(']');

                        await _operation.FlushIfNeeded().ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        await _operation.HandleErrors(_id, e).ConfigureAwait(false);
                    }
                }
            }

            private void WritePrefixForNewCommand()
            {
                _first = true;
                _timeSeriesInBatch = 0;

                _operation._currentWriter.Write("{\"Id\":\"");
                _operation.WriteString(_id);
                _operation._currentWriter.Write("\",\"Type\":\"TimeSeriesBulkInsert\",\"TimeSeries\":{\"Name\":\"");
                _operation.WriteString(_name);
                _operation._currentWriter.Write("\",\"Appends\":[");
            }

            internal static void ThrowAlreadyRunningTimeSeries()
            {
                throw new InvalidOperationException("There is an already running time series operation, did you forget to Dispose it?");
            }

            public void Dispose()
            {
                _operation._inProgressCommand = CommandType.None;

                if (_first == false)
                    _operation._currentWriter.Write("]}}");
            }
        }

        public class TimeSeriesBulkInsert : TimeSeriesBulkInsertBase
        {
            public TimeSeriesBulkInsert(BulkInsertOperation operation, string id, string name) : base(operation, id, name)
            {
            }

            public void Append(DateTime timestamp, double value, string tag = null)
            {
                AsyncHelpers.RunSync(() => AppendAsync(timestamp, new[] { value }, tag));
            }

            public Task AppendAsync(DateTime timestamp, double value, string tag = null)
            {
                return AppendAsyncInternal(timestamp, new[] { value }, tag);
            }

            public void Append(DateTime timestamp, ICollection<double> values, string tag = null)
            {
                AsyncHelpers.RunSync(() => AppendAsync(timestamp, values, tag));
            }

            public Task AppendAsync(DateTime timestamp, ICollection<double> values, string tag = null)
            {
                return AppendAsyncInternal(timestamp, values, tag);
            }
        }

        public class TypedTimeSeriesBulkInsert<TValues> : TimeSeriesBulkInsertBase where TValues : new()
        {
            public TypedTimeSeriesBulkInsert(BulkInsertOperation operation, string id, string name): base(operation, id, name)
            {
            }

            public void Append(DateTime timestamp, TValues value, string tag = null)
            {
                AsyncHelpers.RunSync(() => AppendAsync(timestamp, value, tag));
            }

            public Task AppendAsync(DateTime timestamp, TValues value, string tag = null)
            {
                if (value is ICollection<double> doubles)
                {
                    return AppendAsyncInternal(timestamp, doubles, tag);
                }

                var values = TimeSeriesValuesHelper.GetValues(value).ToArray();
                return AppendAsyncInternal(timestamp, values, tag);
            }

            public void Append(TimeSeriesEntry<TValues> entry)
            {
                AsyncHelpers.RunSync(() => AppendAsync(entry));
            }

            public Task AppendAsync(TimeSeriesEntry<TValues> entry)
            {
                return AppendAsync(entry.Timestamp, entry.Value, entry.Tag);
            }
        }

        public struct AttachmentsBulkInsert
        {
            private readonly BulkInsertOperation _operation;
            private readonly string _id;

            public AttachmentsBulkInsert(BulkInsertOperation operation, string id)
            {
                _operation = operation;
                _id = id;
            }

            public void Store(string name, Stream stream, string contentType = null)
            {
                _operation._attachmentsOperation.Store(_id, name, stream, contentType);
            }

            public Task StoreAsync(string name, Stream stream, string contentType = null, CancellationToken token = default)
            {
                return _operation._attachmentsOperation.StoreAsync(_id, name, stream, contentType, token);
            }
        }

        private class AttachmentsBulkInsertOperation
        {
            private readonly BulkInsertOperation _operation;
            private readonly CancellationToken _token;

            public AttachmentsBulkInsertOperation(BulkInsertOperation operation)
            {
                _operation = operation;
                _token = _operation._token;
            }

            public void Store(string id, string name, Stream stream, string contentType = null)
            {
                AsyncHelpers.RunSync(() => StoreAsync(id, name, stream, contentType, token: default));
            }

            public async Task StoreAsync(string id, string name, Stream stream, string contentType = null, CancellationToken token = default)
            {
                PutAttachmentCommandHelper.ValidateStream(stream);

                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(token, _token);
                using (_operation.ConcurrencyCheck())
                {
                    _operation.EndPreviousCommandIfNeeded();

                    await _operation.ExecuteBeforeStore().ConfigureAwait(false);

                    try
                    {
                        if (_operation._first == false)
                            _operation.WriteComma();

                        _operation._currentWriter.Write("{\"Id\":\"");
                        _operation.WriteString(id);
                        _operation._currentWriter.Write("\",\"Type\":\"AttachmentPUT\",\"Name\":\"");
                        _operation.WriteString(name);

                        if (contentType != null)
                        {
                            _operation._currentWriter.Write("\",\"ContentType\":\"");
                            _operation.WriteString(contentType);
                        }

                        _operation._currentWriter.Write("\",\"ContentLength\":");
                        _operation._currentWriter.Write(stream.Length);
                        _operation._currentWriter.Write('}');
                        await _operation.FlushIfNeeded().ConfigureAwait(false);

                        PutAttachmentCommandHelper.PrepareStream(stream);
                        // pass the default value for bufferSize to make it compile on netstandard2.0
                        await stream.CopyToAsync(_operation._currentWriter.BaseStream, bufferSize: 16 * 1024, cancellationToken: linkedCts.Token).ConfigureAwait(false);

                        await _operation.FlushIfNeeded().ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        await _operation.HandleErrors(id, e).ConfigureAwait(false);
                    }
                }
            }
        }
    }
}
