using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Client.Documents.Smuggler
{
    public class DatabaseSmuggler
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger("Client", typeof(DatabaseSmuggler).FullName);

        private readonly IDocumentStore _store;
        private readonly string _databaseName;
        private readonly RequestExecutor _requestExecutor;

        public DatabaseSmuggler(IDocumentStore store, string databaseName = null)
        {
            _store = store;
            _databaseName = databaseName ?? store.Database;
            if (_databaseName != null)
                _requestExecutor = store.GetRequestExecutor(_databaseName);
        }

        public DatabaseSmuggler(DocumentStore store, string databaseName = null)
            : this((IDocumentStore)store, databaseName)
        {
        }

        public DatabaseSmuggler ForDatabase(string databaseName)
        {
            if (string.Equals(databaseName, _databaseName, StringComparison.OrdinalIgnoreCase))
                return this;

            return new DatabaseSmuggler(_store, databaseName);
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
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations("Could not save export file.", e);

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
            if (_requestExecutor == null)
                throw new InvalidOperationException("Cannot use Smuggler without a database defined, did you forget to call ForDatabase?");

            using (_requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var getOperationIdCommand = new GetNextOperationIdCommand();
                await _requestExecutor.ExecuteAsync(getOperationIdCommand, context, sessionInfo: null, token: token).ConfigureAwait(false);
                var operationId = getOperationIdCommand.Result;

                var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
                token.Register(() => tcs.TrySetCanceled(token));

                var command = new ExportCommand(_requestExecutor.Conventions, context, options, handleStreamResponse, operationId, tcs);
                var requestTask = _requestExecutor.ExecuteAsync(command, context, sessionInfo: null, token: token)
                    .ContinueWith(t =>
                    {
                        if (t.IsFaulted)
                        {
                            tcs.TrySetException(t.Exception);

                            if (Logger.IsOperationsEnabled)
                                Logger.Operations("Could not execute export", t.Exception);
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
                    _requestExecutor,
                    () => _store.Changes(_databaseName),
                    _requestExecutor.Conventions,
                    operationId,
                    null,
                    additionalTask);
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
                await op.WaitForCompletionAsync().ConfigureAwait(false);
            }
            options.OperateOnTypes = oldOperateOnTypes;

            var lastFilePath = Path.Combine(fromDirectory, files.Last());
            var operation = await ImportAsync(options, lastFilePath, cancellationToken).ConfigureAwait(false);
            await operation.WaitForCompletionAsync().ConfigureAwait(false);
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

            try
            {
                if (options == null)
                    throw new ArgumentNullException(nameof(options));
                if (stream == null)
                    throw new ArgumentNullException(nameof(stream));
                if (_requestExecutor == null)
                    throw new InvalidOperationException("Cannot use Smuggler without a database defined, did you forget to call ForDatabase?");

                using (_requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var getOperationIdCommand = new GetNextOperationIdCommand();
                    await _requestExecutor.ExecuteAsync(getOperationIdCommand, context, sessionInfo: null, token: token).ConfigureAwait(false);
                    var operationId = getOperationIdCommand.Result;

                    var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
                    token.Register(() => tcs.TrySetCanceled(token));

                    var command = new ImportCommand(_requestExecutor.Conventions, context, options, stream, operationId, tcs);

                    var requestTask = _requestExecutor.ExecuteAsync(command, context, sessionInfo: null, token: token)
                        .ContinueWith(t =>
                        {
                            using (disposeStream)
                            {
                                if (t.IsFaulted)
                                {
                                    tcs.TrySetException(t.Exception);

                                    if (Logger.IsOperationsEnabled)
                                        Logger.Operations("Could not execute import", t.Exception);
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

                    return new Operation(_requestExecutor, () => _store.Changes(_databaseName), _requestExecutor.Conventions, operationId);
                }
            }
            catch
            {
                disposeStream?.Dispose();

                throw;
            }
        }

        private class ExportCommand : RavenCommand
        {
            private readonly BlittableJsonReaderObject _options;
            private readonly Func<Stream, Task> _handleStreamResponse;
            private readonly long _operationId;
            private readonly TaskCompletionSource<object> _tcs;

            public ExportCommand(DocumentConventions conventions, JsonOperationContext context, DatabaseSmugglerExportOptions options, Func<Stream, Task> handleStreamResponse, long operationId, TaskCompletionSource<object> tcs)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (options == null)
                    throw new ArgumentNullException(nameof(options));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                _handleStreamResponse = handleStreamResponse ?? throw new ArgumentNullException(nameof(handleStreamResponse));
                _options = EntityToBlittable.ConvertCommandToBlittable(options, context);
                _operationId = operationId;
                _tcs = tcs ?? throw new ArgumentNullException(nameof(tcs));
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
                    Content = new BlittableJsonContent(stream =>
                    {
                        ctx.Write(stream, _options);
                        _tcs.TrySetResult(null);
                    })
                };
            }

            public override async Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
            {
                using (var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
                {
                    await _handleStreamResponse(stream).ConfigureAwait(false);
                }

                return ResponseDisposeHandling.Automatic;
            }
        }

        private class ImportCommand : RavenCommand
        {
            private readonly BlittableJsonReaderObject _options;
            private readonly Stream _stream;
            private readonly long _operationId;
            private readonly TaskCompletionSource<object> _tcs;

            public override bool IsReadRequest => false;

            public ImportCommand(DocumentConventions conventions, JsonOperationContext context, DatabaseSmugglerImportOptions options, Stream stream, long operationId, TaskCompletionSource<object> tcs)
            {
                _stream = stream ?? throw new ArgumentNullException(nameof(stream));
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (options == null)
                    throw new ArgumentNullException(nameof(options));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                _options = EntityToBlittable.ConvertCommandToBlittable(options, context);
                _operationId = operationId;
                _tcs = tcs ?? throw new ArgumentNullException(nameof(tcs));
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
                    {new BlittableJsonContent(stream => { ctx.Write(stream, _options); }), Constants.Smuggler.ImportOptions},
                    {new StreamContentWithConfirmation(_stream, _tcs), "file", "name"}
                };

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = form
                };
            }
        }

        private class StreamContentWithConfirmation : StreamContent
        {
            private readonly TaskCompletionSource<object> _tcs;

            public StreamContentWithConfirmation(Stream content, TaskCompletionSource<object> tcs) : base(content)
            {
                _tcs = tcs ?? throw new ArgumentNullException(nameof(tcs));
            }

            protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
            {
                await base.SerializeToStreamAsync(stream, context).ConfigureAwait(false);
                _tcs.TrySetResult(null);
            }
        }

        private class DisposeStreamOnce : IDisposable
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
    }
}
