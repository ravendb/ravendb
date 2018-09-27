using System;
using System.IO;
using System.Linq;
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

namespace Raven.Client.Documents.Smuggler
{
    public class DatabaseSmuggler
    {
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

        public async Task<Operation> ExportAsync(DatabaseSmugglerExportOptions options, string toFile, CancellationToken token = default)
        {
            using (var file = File.OpenWrite(toFile))
            {
                var result = await ExportAsync(options, async stream =>
                {
                    await stream.CopyToAsync(file, 8192, token).ConfigureAwait(false);
                }, token).ConfigureAwait(false);
                await file.FlushAsync(token).ConfigureAwait(false);
                return result;
            }
        }

        private async Task<Operation> ExportAsync(DatabaseSmugglerExportOptions options, Func<Stream, Task> handleStreamResponse, CancellationToken token = default)
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

                var command = new ExportCommand(_requestExecutor.Conventions, context, options, handleStreamResponse, operationId);
                await _requestExecutor.ExecuteAsync(command, context, sessionInfo: null, token: token).ConfigureAwait(false);

                return new Operation(_requestExecutor, () => _store.Changes(_databaseName), _requestExecutor.Conventions, operationId);
            }
        }

        public async Task<Operation> ExportAsync(DatabaseSmugglerExportOptions options, DatabaseSmuggler toDatabase, CancellationToken token = default)
        {
            if (options == null)
                throw new ArgumentNullException(nameof(options));
            if (toDatabase == null)
                throw new ArgumentNullException(nameof(toDatabase));

            var importOptions = new DatabaseSmugglerImportOptions(options);
            var result = await ExportAsync(options, async stream =>
            {
                await toDatabase.ImportAsync(importOptions, stream, token).ConfigureAwait(false);
            }, token).ConfigureAwait(false);
            return result;
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
                await ImportAsync(options, filePath, cancellationToken).ConfigureAwait(false);
            }
            options.OperateOnTypes = oldOperateOnTypes;

            var lastFilePath = Path.Combine(fromDirectory, files.Last());
            await ImportAsync(options, lastFilePath, cancellationToken).ConfigureAwait(false);
        }

        public static DatabaseItemType ConfigureOptionsForIncrementalImport(DatabaseSmugglerOptions options)
        {
            options.OperateOnTypes |= DatabaseItemType.Tombstones;

            // we import the indexes and identities from the last file only, 
            // as the previous files can hold indexes and identities which were deleted and shouldn't be imported
            var oldOperateOnTypes = options.OperateOnTypes;
            options.OperateOnTypes = options.OperateOnTypes &
                                     ~(DatabaseItemType.Indexes | DatabaseItemType.CompareExchange | DatabaseItemType.Identities);
            return oldOperateOnTypes;
        }

        public async Task<Operation> ImportAsync(DatabaseSmugglerImportOptions options, string fromFile, CancellationToken cancellationToken = default)
        {
            var countOfFileParts = 0;
            Operation result;
            do
            {
                using (var fileStream = File.OpenRead(fromFile))
                {
                    result = await ImportAsync(options, fileStream, cancellationToken).ConfigureAwait(false);
                }
                fromFile = $"{fromFile}.part{++countOfFileParts:D3}";
            } while (File.Exists(fromFile));
            return result;
        }

        public async Task<Operation> ImportAsync(DatabaseSmugglerImportOptions options, Stream stream, CancellationToken token = default)
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

                var command = new ImportCommand(_requestExecutor.Conventions, context, options, stream, operationId);
                await _requestExecutor.ExecuteAsync(command, context, sessionInfo: null, token: token).ConfigureAwait(false);

                return new Operation(_requestExecutor, () => _store.Changes(_databaseName), _requestExecutor.Conventions, operationId);
            }
        }

        private class ExportCommand : RavenCommand
        {
            private readonly BlittableJsonReaderObject _options;
            private readonly Func<Stream, Task> _handleStreamResponse;
            private readonly long _operationId;

            public ExportCommand(DocumentConventions conventions, JsonOperationContext context, DatabaseSmugglerExportOptions options, Func<Stream, Task> handleStreamResponse, long operationId)
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

            public override bool IsReadRequest => false;

            public ImportCommand(DocumentConventions conventions, JsonOperationContext context, DatabaseSmugglerImportOptions options, Stream stream, long operationId)
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
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/smuggler/import?operationId={_operationId}";

                var form = new MultipartFormDataContent
                {
                    {new BlittableJsonContent(stream => { ctx.Write(stream, _options); }), "importOptions"},
                    {new StreamContent(_stream), "file", "name"}
                };

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = form
                };
            }
        }
    }
}
