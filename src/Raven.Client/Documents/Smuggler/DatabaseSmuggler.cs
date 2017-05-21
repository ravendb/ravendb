using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Smuggler
{
    public class DatabaseSmuggler
    {
        private readonly DocumentStore _store;
        private readonly string _databaseName;
        private readonly RequestExecutor _requestExecutor;

        public DatabaseSmuggler(DocumentStore store, string databaseName = null)
        {
            _store = store;
            _databaseName = databaseName;
            _requestExecutor = store.GetRequestExecutor(databaseName);
        }

        public DatabaseSmuggler ForDatabase(string databaseName)
        {
            if (string.Equals(databaseName, _databaseName, StringComparison.OrdinalIgnoreCase))
                return this;

            return new DatabaseSmuggler(_store, databaseName);
        }

        public async Task ExportAsync(DatabaseSmugglerOptions options, string toFile, CancellationToken token = default(CancellationToken))
        {
            using (var file = File.OpenWrite(toFile))
            {
                await ExportAsync(options, async stream =>
                {
                    await stream.CopyToAsync(file, 8192, token).ConfigureAwait(false);
                }, token).ConfigureAwait(false);
                await file.FlushAsync(token).ConfigureAwait(false);
            }
        }

        private async Task ExportAsync(DatabaseSmugglerOptions options, Action<Stream> handleStreamResponse, CancellationToken token = default(CancellationToken))
        {
            if (options == null)
                throw new ArgumentNullException(nameof(options));

            using (_requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var command = new ExportCommand(_store.Conventions, context, options, handleStreamResponse);
                await _requestExecutor.ExecuteAsync(command, context, token).ConfigureAwait(false);
            }
        }

        public async Task ExportAsync(DatabaseSmugglerOptions options, DatabaseSmuggler toDatabase, CancellationToken token = default(CancellationToken))
        {
            if (options == null)
                throw new ArgumentNullException(nameof(options));
            if (toDatabase == null)
                throw new ArgumentNullException(nameof(toDatabase));

            await ExportAsync(options, async stream =>
            {
                await toDatabase.ImportAsync(options, stream, token).ConfigureAwait(false);
            }, token).ConfigureAwait(false);
        }

        public async Task ImportIncrementalAsync(DatabaseSmugglerOptions options, string fromDirectory, CancellationToken cancellationToken = default(CancellationToken))
        {
            var files = Directory.GetFiles(fromDirectory)
                .Where(file =>
                {
                    var extension = Path.GetExtension(file);
                    return
                        Constants.Documents.PeriodicExport.IncrementalExportExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                        Constants.Documents.PeriodicExport.FullExportExtension.Equals(extension, StringComparison.OrdinalIgnoreCase);
                })
                .OrderBy(File.GetLastWriteTimeUtc)
                .ToArray();

            if (files.Length == 0)
                return;

            // When we do incremental import, we import the indexes and transformers from the last file only, 
            // as the previous files can hold indexes and transformers which were deleted and shouldn't be imported.
            var oldOperateOnTypes = options.OperateOnTypes;
            options.OperateOnTypes = options.OperateOnTypes & ~(DatabaseItemType.Indexes | DatabaseItemType.Transformers);
            for (var i = 0; i < files.Length - 1; i++)
            {
                var filePath = Path.Combine(fromDirectory, files[i]);
                await ImportAsync(options, filePath, cancellationToken).ConfigureAwait(false);
            }
            options.OperateOnTypes = oldOperateOnTypes;

            var lastFilePath = Path.Combine(fromDirectory, files.Last());
            await ImportAsync(options, lastFilePath, cancellationToken).ConfigureAwait(false);
        }

        public async Task ImportAsync(DatabaseSmugglerOptions options, string fromFile, CancellationToken cancellationToken = default(CancellationToken))
        {
            var countOfFileParts = 0;
            do
            {
                using (var fileStream = File.OpenRead(fromFile))
                {
                    await ImportAsync(options, fileStream, cancellationToken).ConfigureAwait(false);
                }
                fromFile = $"{fromFile}.part{++countOfFileParts:D3}";
            } while (File.Exists(fromFile));
        }

        public async Task ImportAsync(DatabaseSmugglerOptions options, Stream stream, CancellationToken token = default(CancellationToken))
        {
            if (options == null)
                throw new ArgumentNullException(nameof(options));
            if (stream == null)
                throw new ArgumentNullException(nameof(stream));

            JsonOperationContext context;
            using (_requestExecutor.ContextPool.AllocateOperationContext(out context))
            {
                var command = new ImportCommand(options, stream);

                await _requestExecutor.ExecuteAsync(command, context, token).ConfigureAwait(false);
            }
        }

        private class ExportCommand : RavenCommand
        {
            private readonly JsonOperationContext _context;
            private readonly BlittableJsonReaderObject _options;
            private readonly Action<Stream> _handleStreamResponse;

            public ExportCommand(DocumentConventions conventions, JsonOperationContext context, DatabaseSmugglerOptions options, 
                Action<Stream> handleStreamResponse)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (options == null)
                    throw new ArgumentNullException(nameof(options));

                _context = context ?? throw new ArgumentNullException(nameof(context));
                _handleStreamResponse = handleStreamResponse ?? throw new ArgumentNullException(nameof(handleStreamResponse));
                _options = EntityToBlittable.ConvertEntityToBlittable(options, conventions, _context);

                ResponseType = RavenCommandResponseType.Raw;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/smuggler/export";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        _context.Write(stream, _options);
                    })
                };
            }

            public override void SetResponseRaw(HttpResponseMessage response, Stream stream, JsonOperationContext context)
            {
                _handleStreamResponse(stream);
            }
        }

        private class ImportCommand : RavenCommand
        {
            private readonly DatabaseSmugglerOptions _options;
            private readonly Stream _stream;

            public ImportCommand(DatabaseSmugglerOptions options, Stream stream)
            {
                _options = options ?? throw new ArgumentNullException(nameof(options));
                _stream = stream ?? throw new ArgumentNullException(nameof(stream));
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/smuggler/import?{_options.ToQueryString()}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new StreamContent(_stream)
                };
            }
        }
    }
}