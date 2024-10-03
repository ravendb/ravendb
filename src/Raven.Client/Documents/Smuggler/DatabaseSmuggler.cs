using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Logging;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Client.Documents.Smuggler
{
    public sealed class DatabaseSmuggler
    {
        private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForClient<DatabaseSmuggler>();

        private readonly Func<string, string, ISingleNodeDatabaseChanges> _getChanges;
        private readonly Func<string, RequestExecutor> _getRequestExecutor;
        private readonly string _databaseName;
        private RequestExecutor _requestExecutor;
        private RequestExecutor RequestExecutor => _requestExecutor ?? (_databaseName != null ? _requestExecutor = _getRequestExecutor(_databaseName) : null);

        public DatabaseSmuggler(IDocumentStore store, string databaseName = null) 
            : this(store.Changes, store.GetRequestExecutor, databaseName ?? store.Database)
        {
        }

        public DatabaseSmuggler(DocumentStore store, string databaseName = null)
            : this((IDocumentStore)store, databaseName)
        {
        }

        internal DatabaseSmuggler(Func<string, string, ISingleNodeDatabaseChanges> getChanges, Func<string, RequestExecutor> getRequestExecutor, string databaseName)
        {
            _getChanges = getChanges;
            _databaseName = databaseName;
            _getRequestExecutor = getRequestExecutor;
        }

        public DatabaseSmuggler ForDatabase(string databaseName)
        {
            if (string.Equals(databaseName, _databaseName, StringComparison.OrdinalIgnoreCase))
                return this;

            return new DatabaseSmuggler(_getChanges, _getRequestExecutor, databaseName);
        }

        internal Task<Operation> ExportToStreamAsync(DatabaseSmugglerExportOptions options, Func<Stream, Task> handleStreamResponse, CancellationToken token = default)
        {
            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            return ExportAsync(options, async stream =>
            {
                try
                {
                    await handleStreamResponse(stream).ConfigureAwait(false);
                    
                    tcs.TrySetResult(null);
                }
                catch (Exception e)
                {
                    tcs.TrySetException(e);
                }
            }, tcs.Task, token);
        }

        public Task<Operation> ExportAsync(DatabaseSmugglerExportOptions options, string toFile, CancellationToken token = default)
        {
            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            return ExportAsync(options, async stream =>
            {
                try
                {
                    var fileInfo = new FileInfo(toFile);
                    var directoryInfo = fileInfo.Directory;
                    if (directoryInfo != null && directoryInfo.Exists == false)
                        directoryInfo.Create();

                    using (var fileStream = fileInfo.OpenWrite())
                        await stream.CopyToAsync(fileStream, 8192, token).ConfigureAwait(false);

                    tcs.TrySetResult(null);
                }
                catch (Exception e)
                {
                    if (Logger.IsErrorEnabled)
                        Logger.Error("Could not save export file.", e);

                    tcs.TrySetException(e);

                    if (e is UnauthorizedAccessException || e is DirectoryNotFoundException || e is IOException)
                        throw new InvalidOperationException($"Cannot export to selected path {toFile}, please ensure you selected proper filename.", e);

                    throw new InvalidOperationException($"Could not save export file {toFile}.", e);
                }
            }, tcs.Task, token);
        }

        private async Task<Operation> ExportAsync(DatabaseSmugglerExportOptions options, Func<Stream, Task> handleStreamResponse, Task additionalTask, CancellationToken token = default)
        {
            if (options == null)
                throw new ArgumentNullException(nameof(options));
            if (RequestExecutor == null)
                throw new InvalidOperationException("Cannot use Smuggler without a database defined, did you forget to call ForDatabase?");

            IDisposable returnContext = null;
            Task requestTask = null;

            try
            {
                returnContext = RequestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context);
                var getOperationIdCommand = new GetNextOperationIdCommand();
                await RequestExecutor.ExecuteAsync(getOperationIdCommand, context, sessionInfo: null, token: token).ConfigureAwait(false);
                var operationId = getOperationIdCommand.Result;

                var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
                var cancellationTokenRegistration = token.Register(() => tcs.TrySetCanceled(token));

                var command = new ExportCommand(RequestExecutor.Conventions, context, options, handleStreamResponse, operationId, tcs, getOperationIdCommand.NodeTag);

                requestTask = RequestExecutor.ExecuteAsync(command, context, sessionInfo: null, token: token)
                    .ContinueWith(t =>
                    {
                        cancellationTokenRegistration.Dispose();
                        returnContext?.Dispose();
                        if (t.IsFaulted)
                        {
                            tcs.TrySetException(t.Exception);

                            if (Logger.IsErrorEnabled)
                                Logger.Error("Could not execute export", t.Exception);
                        }
                    }, token);

                try
                {
                    await tcs.Task.ConfigureAwait(false);
                }
                catch (Exception)
                {
                    await requestTask.ConfigureAwait(false);
                    await tcs.Task.ConfigureAwait(false);
                }

                return new Operation(
                    RequestExecutor,
                    () => _getChanges(_databaseName, getOperationIdCommand.NodeTag),
                    RequestExecutor.Conventions,
                    operationId,
                    getOperationIdCommand.NodeTag,
                    additionalTask);
            }
            catch (Exception e)
            {
                if (requestTask == null)
                    returnContext?.Dispose();

                throw e.ExtractSingleInnerException();
            }
        }

        private Task<Operation> ExportAsync(DatabaseSmugglerExportOptions options, Func<Stream, Task> handleStreamResponse, CancellationToken token = default)
        {
            return ExportAsync(options, handleStreamResponse, null, token);
        }

        public async Task<Operation> ExportAsync(DatabaseSmugglerExportOptions options, DatabaseSmuggler toDatabase, CancellationToken token = default)
        {
            if (options == null)
                throw new ArgumentNullException(nameof(options));
            if (toDatabase == null)
                throw new ArgumentNullException(nameof(toDatabase));

            Operation operation = null;
            var importOptions = new DatabaseSmugglerImportOptions(options);

            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

            await ExportAsync(options, async stream =>
            {
                try
                {
                    operation = await toDatabase.ImportAsync(importOptions, stream, token).ConfigureAwait(false);
                    tcs.TrySetResult(null);
                }
                catch (Exception e)
                {
                    tcs.TrySetException(e);
                    throw;
                }
            }, token).ConfigureAwait(false);

            await tcs.Task.ConfigureAwait(false);

            return operation;
        }

        public async Task ImportIncrementalAsync(DatabaseSmugglerImportOptions options, string fromDirectory, CancellationToken cancellationToken = default)
        {
            var files = Directory.GetFiles(fromDirectory)
                .Where(BackupUtils.IsBackupFile)
                .OrderBackups()
                .ToArray();

            if (files.Length == 0)
                return;

            var oldOperateOnTypes = ConfigureOptionsForIncrementalImport(options);
            for (var i = 0; i < files.Length - 1; i++)
            {
                var filePath = Path.Combine(fromDirectory, files[i]);
                var op = await ImportAsync(options, filePath, cancellationToken).ConfigureAwait(false);
                await op.WaitForCompletionAsync().WithCancellation(cancellationToken).ConfigureAwait(false);
            }
            options.OperateOnTypes = oldOperateOnTypes;

            var lastFilePath = Path.Combine(fromDirectory, files.Last());
            var operation = await ImportAsync(options, lastFilePath, cancellationToken).ConfigureAwait(false);
            await operation.WaitForCompletionAsync().WithCancellation(cancellationToken).ConfigureAwait(false);
        }

        internal static DatabaseItemType ConfigureOptionsForIncrementalImport(DatabaseSmugglerOptions options)
        {
            options.OperateOnTypes |= DatabaseItemType.Tombstones;
            options.OperateOnTypes |= DatabaseItemType.CompareExchangeTombstones;

            // we import the indexes and Subscriptions from the last file only,
            var oldOperateOnTypes = options.OperateOnTypes;
            options.OperateOnTypes &= ~DatabaseItemType.Indexes;
            options.OperateOnTypes &= ~DatabaseItemType.Subscriptions;
            return oldOperateOnTypes;
        }

        public Task<Operation> ImportAsync(DatabaseSmugglerImportOptions options, string fromFile, CancellationToken cancellationToken = default)
        {
            return ImportInternalAsync(options, File.OpenRead(fromFile), leaveOpen: false, cancellationToken);
        }

        public Task<Operation> ImportAsync(DatabaseSmugglerImportOptions options, Stream stream, CancellationToken token = default)
        {
            return ImportInternalAsync(options, stream, leaveOpen: true, token);
        }

        private async Task<Operation> ImportInternalAsync(DatabaseSmugglerImportOptions options, Stream stream, bool leaveOpen, CancellationToken token = default)
        {
            var disposeStream = leaveOpen ? null : new DisposeStreamOnce(stream);
            IDisposable returnContext = null;
            Task requestTask = null;

            try
            {
                if (options == null)
                    throw new ArgumentNullException(nameof(options));
                if (stream == null)
                    throw new ArgumentNullException(nameof(stream));
                if (RequestExecutor == null)
                    throw new InvalidOperationException("Cannot use Smuggler without a database defined, did you forget to call ForDatabase?");

                returnContext = RequestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context);
                var getOperationIdCommand = new GetNextOperationIdCommand();
                await RequestExecutor.ExecuteAsync(getOperationIdCommand, context, sessionInfo: null, token: token).ConfigureAwait(false);
                var operationId = getOperationIdCommand.Result;

                var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
                var cancellationTokenRegistration = token.Register(() => tcs.TrySetCanceled(token));

                var command = new ImportCommand(RequestExecutor.Conventions, context, options, stream, operationId, tcs, this, getOperationIdCommand.NodeTag);

                var task = RequestExecutor.ExecuteAsync(command, context, sessionInfo: null, token: token);
                requestTask = task
                        .ContinueWith(t =>
                        {
                            returnContext?.Dispose();
                            cancellationTokenRegistration.Dispose();
                            using (disposeStream)
                            {
                                if (t.IsFaulted)
                                {
                                    tcs.TrySetException(t.Exception);

                                    if (Logger.IsErrorEnabled)
                                        Logger.Error("Could not execute import", t.Exception);
                                }
                            }
                        }, token);

                try
                {
                    await tcs.Task.ConfigureAwait(false);
                }
                catch (Exception)
                {
                    await requestTask.ConfigureAwait(false);
                    await tcs.Task.ConfigureAwait(false);
                }

                return new Operation(RequestExecutor, () => _getChanges(_databaseName, getOperationIdCommand.NodeTag), RequestExecutor.Conventions, operationId,
                    nodeTag: getOperationIdCommand.NodeTag, afterOperationCompleted: task);

            }
            catch (Exception e)
            {
                if (requestTask == null)
                {
                    // handle the possible double dispose of return context
                    returnContext?.Dispose();
                }

                disposeStream?.Dispose();
                throw e.ExtractSingleInnerException();
            }
        }

        private sealed class ExportCommand : RavenCommand
        {
            private readonly BlittableJsonReaderObject _options;
            private readonly DocumentConventions _conventions;
            private readonly Func<Stream, Task> _handleStreamResponse;
            private readonly long _operationId;
            private readonly TaskCompletionSource<object> _tcs;

            public ExportCommand(DocumentConventions conventions, JsonOperationContext context, DatabaseSmugglerExportOptions options, Func<Stream, Task> handleStreamResponse, long operationId, TaskCompletionSource<object> tcs, string nodeTag)
            {
                if (options == null)
                    throw new ArgumentNullException(nameof(options));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _handleStreamResponse = handleStreamResponse ?? throw new ArgumentNullException(nameof(handleStreamResponse));
                _options = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(options, context);
                _operationId = operationId;
                _tcs = tcs ?? throw new ArgumentNullException(nameof(tcs));
                SelectedNodeTag = nodeTag;
            }

            public override void OnResponseFailure(HttpResponseMessage response)
            {
                _tcs.TrySetCanceled();
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/smuggler/export?operationId={_operationId}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await ctx.WriteAsync(stream, _options).ConfigureAwait(false);
                        _tcs.TrySetResult(null);
                    }, _conventions)
                };
            }

            public override async Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
            {
                using (var stream = await response.Content.ReadAsStreamWithZstdSupportAsync().ConfigureAwait(false))
                {
                    await _handleStreamResponse(stream).ConfigureAwait(false);
                }

                return ResponseDisposeHandling.Automatic;
            }
        }

        private sealed class ImportCommand : RavenCommand
        {
            private readonly BlittableJsonReaderObject _options;
            private readonly DocumentConventions _conventions;
            private readonly Stream _stream;
            private readonly long _operationId;
            private readonly TaskCompletionSource<object> _tcs;
            private readonly DatabaseSmuggler _parent;

            public override bool IsReadRequest => false;

            public ImportCommand(DocumentConventions conventions, JsonOperationContext context, DatabaseSmugglerImportOptions options, Stream stream, long operationId, TaskCompletionSource<object> tcs, DatabaseSmuggler parent, string nodeTag)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _stream = stream ?? throw new ArgumentNullException(nameof(stream));
                if (options == null)
                    throw new ArgumentNullException(nameof(options));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                _options = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(options, context);
                _operationId = operationId;
                _tcs = tcs ?? throw new ArgumentNullException(nameof(tcs));
                _parent = parent ?? throw new ArgumentNullException(nameof(parent));
                SelectedNodeTag = nodeTag;
            }

            public override void OnResponseFailure(HttpResponseMessage response)
            {
                _tcs.TrySetCanceled();
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/smuggler/import?operationId={_operationId}";

                var form = new MultipartFormDataContent
                {
                    {new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _options).ConfigureAwait(false), _conventions), Constants.Smuggler.ImportOptions},
                    {new StreamContentWithConfirmation(_stream, _tcs, _parent), "file", "name"}
                };

                return new HttpRequestMessage
                {
                    Headers =
                    {
                        TransferEncodingChunked = true
                    },
                    Method = HttpMethod.Post,
                    Content = form
                };
            }
        }

        internal sealed class StreamContentWithConfirmation : StreamContent
        {
            private readonly TaskCompletionSource<object> _tcs;
            private readonly DatabaseSmuggler _parent;

            public StreamContentWithConfirmation(Stream content, TaskCompletionSource<object> tcs, DatabaseSmuggler parent = null) : base(content)
            {
                _tcs = tcs ?? throw new ArgumentNullException(nameof(tcs));
                _parent = parent;
            }

            protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
            {
                _parent?.ForTestingPurposes?.BeforeSerializeToStreamAsync?.Invoke();

                // Immediately flush request stream to send headers
                // https://github.com/dotnet/corefx/issues/39586#issuecomment-516210081
                // https://github.com/dotnet/runtime/issues/96223#issuecomment-1865009861
                await stream.FlushAsync().ConfigureAwait(false);

                await base.SerializeToStreamAsync(stream, context).ConfigureAwait(false);
                _tcs.TrySetResult(null);
            }
        }

        private sealed class DisposeStreamOnce : IDisposable
        {
            private readonly Stream _stream;

            private bool _disposed;

            public DisposeStreamOnce(Stream stream)
            {
                _stream = stream;
            }

            public void Dispose()
            {
                if (_disposed)
                    return;

                lock (this)
                {
                    if (_disposed)
                        return;

                    _disposed = true;

                    using (_stream)
                    {
                    }
                }
            }
        }

        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff();
        }

        internal sealed class TestingStuff
        {
            internal Action BeforeSerializeToStreamAsync;

            internal IDisposable CallBeforeSerializeToStreamAsync(Action action)
            {
                BeforeSerializeToStreamAsync = action;

                return new DisposableAction(() => BeforeSerializeToStreamAsync = null);
            }
        }
    }
}
