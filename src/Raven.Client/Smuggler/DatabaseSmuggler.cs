using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Sparrow.Json;

namespace Raven.NewClient.Client.Smuggler
{
    public class DatabaseSmuggler
    {
        private readonly DocumentStore _store;
        private readonly string _databaseName;
        private readonly RequestExecuter _requestExecuter;

        public DatabaseSmuggler(DocumentStore store, string databaseName = null)
        {
            _store = store;
            _databaseName = databaseName;
            _requestExecuter = store.GetRequestExecuter(databaseName);
        }

        public DatabaseSmuggler ForDatabase(string databaseName)
        {
            if (string.Equals(databaseName, _databaseName, StringComparison.OrdinalIgnoreCase))
                return this;

            return new DatabaseSmuggler(_store, databaseName);
        }

        public async Task ExportAsync(DatabaseSmugglerOptions options, string toFile, CancellationToken token = default(CancellationToken))
        {
            using (var stream = await ExportAsync(options, token).ConfigureAwait(false))
            using (var file = File.OpenWrite(toFile))
            {
                await stream.CopyToAsync(file, 8192, token).ConfigureAwait(false);
                await file.FlushAsync(token).ConfigureAwait(false);
            }
        }

        private async Task<Stream> ExportAsync(DatabaseSmugglerOptions options, CancellationToken token)
        {
            if (options == null)
                throw new ArgumentNullException(nameof(options));

            JsonOperationContext context;
            using (_requestExecuter.ContextPool.AllocateOperationContext(out context))
            {
                var command = new ExportCommand(_store.Conventions, context, options);

                await _requestExecuter.ExecuteAsync(command, context, token).ConfigureAwait(false);

                return command.Result;
            }
        }

        public async Task ExportAsync(DatabaseSmugglerOptions options, DatabaseSmuggler toDatabase, CancellationToken token = default(CancellationToken))
        {
            if (options == null)
                throw new ArgumentNullException(nameof(options));
            if (toDatabase == null)
                throw new ArgumentNullException(nameof(toDatabase));

            using (var stream = await ExportAsync(options, token).ConfigureAwait(false))
            {
                await toDatabase.ImportAsync(options, stream, token).ConfigureAwait(false);
            }
        }

        public async Task ImportIncrementalAsync(DatabaseSmugglerOptions options, string fromDirectory, CancellationToken cancellationToken = default(CancellationToken))
        {
            var files = Directory.GetFiles(fromDirectory)
                .Where(file =>
                {
                    var extension = Path.GetExtension(file);
                    return
                        Constants.PeriodicExport.IncrementalExportExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                        Constants.PeriodicExport.FullExportExtension.Equals(extension, StringComparison.OrdinalIgnoreCase);
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
            using (_requestExecuter.ContextPool.AllocateOperationContext(out context))
            {
                var command = new ImportCommand(options, stream);

                await _requestExecuter.ExecuteAsync(command, context, token).ConfigureAwait(false);
            }
        }

        private class ExportCommand : RavenCommand<Stream>
        {
            private readonly JsonOperationContext _context;
            private readonly BlittableJsonReaderObject _options;

            public ExportCommand(DocumentConvention conventions, JsonOperationContext context, DatabaseSmugglerOptions options)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                if (options == null)
                    throw new ArgumentNullException(nameof(options));

                _context = context;
                _options = new EntityToBlittable(null).ConvertEntityToBlittable(options, conventions, _context); ;
            }

            public override bool IsReadRequest => false;

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

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                ThrowInvalidResponse();
            }

            public override async Task ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
            {
                Result = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
            }
        }

        private class ImportCommand : RavenCommand<object>
        {
            private readonly DatabaseSmugglerOptions _options;
            private readonly Stream _stream;

            public ImportCommand(DatabaseSmugglerOptions options, Stream stream)
            {
                if (options == null)
                    throw new ArgumentNullException(nameof(options));
                if (stream == null)
                    throw new ArgumentNullException(nameof(stream));

                _options = options;
                _stream = stream;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/smuggler/import?{_options.ToQueryString()}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new StreamContent(_stream)
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
            }
        }
    }
}