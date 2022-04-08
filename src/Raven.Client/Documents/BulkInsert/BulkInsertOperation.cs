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
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Json;
using Sparrow.Threading;

namespace Raven.Client.Documents.BulkInsert
{
    public class BulkInsertOperation : BulkInsertOperationBase<object>, IDisposable, IAsyncDisposable
    {
        private readonly BulkInsertOptions _options;
        private readonly CancellationToken _token;
        private readonly GenerateEntityIdOnTheClient _generateEntityIdOnTheClient;

        internal class BulkInsertStreamExposerContent : StreamExposerContent
        {
            public void Done()
            {
                if (Complete() == false)
                {
                    throw new BulkInsertProtocolViolationException("Unable to close the stream", _done.Task.Exception);
                }
            }
        }

        internal class BulkInsertCommand : RavenCommand<HttpResponseMessage>
        {
            public override bool IsReadRequest => false;
            private readonly BulkInsertStreamExposerContent _stream;
            private readonly bool _skipOverwriteIfUnchanged;
            private readonly long _id;

            public BulkInsertCommand(long id, BulkInsertStreamExposerContent stream, string nodeTag, bool skipOverwriteIfUnchanged)
            {
                _id = id;
                _stream = stream;
                SelectedNodeTag = nodeTag;
                _skipOverwriteIfUnchanged = skipOverwriteIfUnchanged;
                Timeout = TimeSpan.FromHours(12); // global max timeout
            }

            internal string RequestNodeTag { get; private set; }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                RequestNodeTag = node.ClusterTag;

                url = $"{node.Url}/databases/{node.Database}/bulk_insert?id={_id}&skipOverwriteIfUnchanged={_skipOverwriteIfUnchanged}";
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

        private readonly JsonOperationContext _context;
        private readonly IDisposable _resetContext;

        private CommandType _inProgressCommand;
        private readonly CountersBulkInsertOperation _countersOperation;
        private readonly AttachmentsBulkInsertOperation _attachmentsOperation;
        private string _nodeTag;

        private readonly IJsonSerializer _defaultSerializer;
        private readonly Func<object, IMetadataDictionary, StreamWriter, bool> _customEntitySerializer;
        private readonly int _timeSeriesBatchSize;
        private long _concurrentCheck;
        private bool _first = true;

        public CompressionLevel CompressionLevel = CompressionLevel.NoCompression;

        public BulkInsertOperation(string database, IDocumentStore store, BulkInsertOptions options, CancellationToken token = default)
        {
            CompressionLevel = options?.CompressionLevel ?? CompressionLevel.NoCompression;
            _options = options ?? new BulkInsertOptions();
            _token = token;
            _conventions = store.Conventions;
            if (string.IsNullOrWhiteSpace(database))
                ThrowNoDatabase();
            _requestExecutor = store.GetRequestExecutor(database);
            _resetContext = _requestExecutor.ContextPool.AllocateOperationContext(out _context);
            _writer = new BulkInsertWriter(_context, _token);
            _countersOperation = new CountersBulkInsertOperation(this);
            _attachmentsOperation = new AttachmentsBulkInsertOperation(this);

            _defaultSerializer = _requestExecutor.Conventions.Serialization.CreateSerializer();
            _customEntitySerializer = _requestExecutor.Conventions.BulkInsert.TrySerializeEntityToJsonStream;
            _timeSeriesBatchSize = _conventions.BulkInsert.TimeSeriesBatchSize;

            _generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(_requestExecutor.Conventions,
                entity => AsyncHelpers.RunSync(() => _requestExecutor.Conventions.GenerateDocumentIdAsync(database, entity)));

            _disposeOnce = new DisposeOnceAsync<SingleAttempt>(async () =>
            {
                try
                {
                    if (_writer.StreamExposer.IsDone)
                        return;

                    EndPreviousCommandIfNeeded();

                    Exception flushEx = null;

                    try
                    {
                        await _writer.DisposeAsync().ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        flushEx = e;
                    }

                    if (OperationId == -1)
                    {
                        // closing without calling a single store.
                        return;
                    }

                    if (BulkInsertExecuteTask != null)
                    {
                        try
                        {
                            await BulkInsertExecuteTask.ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            await ThrowBulkInsertAborted(e, flushEx).ConfigureAwait(false);
                        }
                    }
                }
                finally
                {
                    _resetContext.Dispose();
                }
            });
        }

        public BulkInsertOperation(string database, IDocumentStore store, CancellationToken token = default) : this(database, store, null, token)
        {
            
        }

        private void ThrowNoDatabase()
        {
            throw new InvalidOperationException(
                $"Cannot start bulk insert operation without specifying a name of a database to operate on. " +
                $"Database name can be passed as an argument when bulk insert is being created or default database can be defined using '{nameof(DocumentStore)}.{nameof(IDocumentStore.Database)}' property.");
        }

        protected override async Task WaitForId()
        {
            if (OperationId != -1)
                return;

            var bulkInsertGetIdRequest = new GetNextOperationIdCommand();
            await ExecuteAsync(bulkInsertGetIdRequest, token: _token).ConfigureAwait(false);
            OperationId = bulkInsertGetIdRequest.Result;
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

        public override Task StoreAsync(object entity, string id)
        {
            return StoreAsync(entity, id, null);
        }

        public async Task StoreAsync(object entity, string id, IMetadataDictionary metadata)
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

                    _writer.Write("{\"Id\":\"");
                    WriteString(id);
                    _writer.Write("\",\"Type\":\"PUT\",\"Document\":");

                    await _writer.FlushAsync().ConfigureAwait(false);

                    if (_customEntitySerializer == null || _customEntitySerializer(entity, metadata, _writer.StreamWriter) == false)
                    {
                        using (var json = _conventions.Serialization.DefaultConverter.ToBlittable(entity, metadata, _context, _defaultSerializer))
                            await json.WriteJsonToAsync(_writer.StreamWriter.BaseStream, _token).ConfigureAwait(false);
                    }

                    _writer.Write('}');

                    await _writer.FlushIfNeeded().ConfigureAwait(false);
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
                        _writer.Write('\\');
                }

                _writer.Write(c);
            }
        }

        private void WriteComma()
        {
            _writer.Write(',');
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

        protected override async Task<BulkInsertAbortedException> GetExceptionFromOperation()
        {
            var stateRequest = new GetOperationStateOperation.GetOperationStateCommand(OperationId, _nodeTag);
            await ExecuteAsync(stateRequest, token: _token).ConfigureAwait(false);

            if (!(stateRequest.Result?.Result is OperationExceptionResult error))
                return null;
            return new BulkInsertAbortedException(error.Error);
        }

        private readonly BulkInsertWriter _writer;

        protected override async Task EnsureStreamAsync()
        {
            if (CompressionLevel != CompressionLevel.NoCompression)
                _writer.StreamExposer.Headers.ContentEncoding.Add("gzip");

            var bulkCommand = new BulkInsertCommand(
                OperationId,
                _writer.StreamExposer,
                _nodeTag,
                _options.SkipOverwriteIfUnchanged);

            BulkInsertExecuteTask = ExecuteAsync(bulkCommand);

            await _writer.EnsureStreamAsync(CompressionLevel).ConfigureAwait(false);
        }

        private async Task ExecuteAsync(BulkInsertCommand cmd)
        {
            try
            {
                await ExecuteAsync(cmd, token: _token).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _writer.StreamExposer.ErrorOnRequestStart(e);
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
            _writer.StreamExposer.ErrorOnProcessingRequest(
                new BulkInsertAbortedException($"Write to stream failed at document with id {id}.", innerEx));
            await BulkInsertExecuteTask.ConfigureAwait(false);
        }

        public override async Task AbortAsync()
        {
            if (OperationId == -1)
                return; // nothing was done, nothing to kill
            await WaitForId().ConfigureAwait(false);
            try
            {
                await ExecuteAsync(new KillOperationCommand(OperationId, _nodeTag), token: _token).ConfigureAwait(false);
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
                                _operation._writer.Write("]}},");
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
                            _operation._writer.Write("]}},");

                            WritePrefixForNewCommand();
                        }

                        _countersInBatch++;

                        if (_first == false)
                        {
                            _operation.WriteComma();
                        }

                        _first = false;

                        _operation._writer.Write("{\"Type\":\"Increment\",\"CounterName\":\"");
                        _operation.WriteString(name);
                        _operation._writer.Write("\",\"Delta\":");
                        _operation._writer.Write(delta);
                        _operation._writer.Write('}');

                        await _operation._writer.FlushIfNeeded().ConfigureAwait(false);
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

                _operation._writer.Write("]}}");
                _id = null;
            }

            private void WritePrefixForNewCommand()
            {
                _first = true;
                _countersInBatch = 0;

                _operation._writer.Write("{\"Id\":\"");
                _operation.WriteString(_id);
                _operation._writer.Write("\",\"Type\":\"Counters\",\"Counters\":{\"DocumentId\":\"");
                _operation.WriteString(_id);
                _operation._writer.Write("\",\"Operations\":[");
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
                            _operation._writer.Write("]}},");
                            WritePrefixForNewCommand();
                        }

                        _timeSeriesInBatch++;

                        if (_first == false)
                        {
                            _operation.WriteComma();
                        }

                        _first = false;

                        _operation._writer.Write('[');

                        timestamp = timestamp.EnsureUtc();
                        _operation._writer.Write(timestamp.Ticks);
                        _operation.WriteComma();

                        _operation._writer.Write(values.Count);
                        _operation.WriteComma();

                        var firstValue = true;
                        foreach (var value in values)
                        {
                            if (firstValue == false)
                                _operation.WriteComma();

                            firstValue = false;
                            _operation._writer.Write(value.ToString("R", CultureInfo.InvariantCulture));
                        }

                        if (tag != null)
                        {
                            _operation._writer.Write(",\"");
                            _operation.WriteString(tag);
                            _operation._writer.Write('\"');
                        }

                        _operation._writer.Write(']');

                        await _operation._writer.FlushIfNeeded().ConfigureAwait(false);
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

                _operation._writer.Write("{\"Id\":\"");
                _operation.WriteString(_id);
                _operation._writer.Write("\",\"Type\":\"TimeSeriesBulkInsert\",\"TimeSeries\":{\"Name\":\"");
                _operation.WriteString(_name);
                _operation._writer.Write("\",\"Appends\":[");
            }

            internal static void ThrowAlreadyRunningTimeSeries()
            {
                throw new InvalidOperationException("There is an already running time series operation, did you forget to Dispose it?");
            }

            public void Dispose()
            {
                _operation._inProgressCommand = CommandType.None;

                if (_first == false)
                    _operation._writer.Write("]}}");
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

                        _operation._writer.Write("{\"Id\":\"");
                        _operation.WriteString(id);
                        _operation._writer.Write("\",\"Type\":\"AttachmentPUT\",\"Name\":\"");
                        _operation.WriteString(name);

                        if (contentType != null)
                        {
                            _operation._writer.Write("\",\"ContentType\":\"");
                            _operation.WriteString(contentType);
                        }

                        _operation._writer.Write("\",\"ContentLength\":");
                        _operation._writer.Write(stream.Length);
                        _operation._writer.Write('}');
                        await _operation._writer.FlushIfNeeded().ConfigureAwait(false);

                        PutAttachmentCommandHelper.PrepareStream(stream);
                        // pass the default value for bufferSize to make it compile on netstandard2.0
                        await stream.CopyToAsync(_operation._writer.StreamWriter.BaseStream, bufferSize: 16 * 1024, cancellationToken: linkedCts.Token).ConfigureAwait(false);

                        await _operation._writer.FlushIfNeeded().ConfigureAwait(false);
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
