using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Json;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Smuggler
{
    public class DatabaseSmuggler
    {
        private readonly DocumentStore _store;

        public DatabaseSmuggler(DocumentStore store)
        {
            _store = store;
        }

        public async Task ExportAsync(DatabaseSmugglerOptions options, string destinationFilePath, CancellationToken token = default(CancellationToken))
        {
            using (var stream = await ExportAsync(options, token))
            using (var file = File.OpenWrite(destinationFilePath))
            {
                await stream.CopyToAsync(file, 8192, token);
                await file.FlushAsync(token);
            }
        }

        private async Task<Stream> ExportAsync(DatabaseSmugglerOptions options, CancellationToken token)
        {
            var httpClient = GetHttpClient();
            ShowProgress("Starting to export file");

            var database = options.Database ?? _store.DefaultDatabase;
            var url = $"{_store.Url}/databases/{database}/smuggler/export";
            var json = RavenJObject.FromObject(options);

            var response = await httpClient.PostAsync(url, new StringContent(json.ToString()), token).ConfigureAwait(false);
            if (response.IsSuccessStatusCode == false)
                throw new InvalidOperationException(await response.Content.ReadAsStringAsync());
            var stream = await response.Content.ReadAsStreamAsync();
            return stream;
        }

        public async Task ExportAsync(DatabaseSmugglerOptions options, string serverUrl, string databaseName, CancellationToken token = default(CancellationToken))
        {
            using (var stream = await ExportAsync(options, token))
            {
                await ImportAsync(options, stream, serverUrl, databaseName, token);
            }
        }

        public async Task ImportIncrementalAsync(DatabaseSmugglerOptions options,  string directoryPath, CancellationToken cancellationToken = default(CancellationToken))
        {
            var files = Directory.GetFiles(directoryPath)
                .Where(file => Constants.PeriodicExport.IncrementalExportExtension.Equals(Path.GetExtension(file), StringComparison.OrdinalIgnoreCase))
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
                var filePath = Path.Combine(directoryPath, files[i]);
                await ImportAsync(options, filePath, cancellationToken).ConfigureAwait(false);
            }
            options.OperateOnTypes = oldOperateOnTypes;

            var lastFilePath = Path.Combine(directoryPath, files.Last());
            await ImportAsync(options, lastFilePath, cancellationToken).ConfigureAwait(false);
        }

        public async Task ImportAsync(DatabaseSmugglerOptions options, string filePath, CancellationToken cancellationToken = default(CancellationToken))
        {
            var countOfFileParts = 0;
            do
            {
                ShowProgress($"Starting to import file: {filePath}");
                using (var fileStream = File.OpenRead(filePath))
                {
                    await ImportAsync(options, fileStream, _store.Url, options.Database ?? _store.DefaultDatabase, cancellationToken).ConfigureAwait(false);
                }
                filePath = $"{filePath}.part{++countOfFileParts:D3}";
            } while (File.Exists(filePath));
        }

        public async Task ImportAsync(DatabaseSmugglerOptions options, Stream stream, CancellationToken cancellationToken = default(CancellationToken))
        {
            ShowProgress("Starting to import from stream");
            await ImportAsync(options, stream, _store.Url, options.Database ?? _store.DefaultDatabase,
                cancellationToken).ConfigureAwait(false);
        }

        private async Task ImportAsync(DatabaseSmugglerOptions options, Stream stream, string url, string database, CancellationToken cancellationToken)
        {
            var httpClient = GetHttpClient();
            using (var content = new StreamContent(stream))
            {
                var uri = $"{url}/databases/{database}/smuggler/import";
                var query = new Dictionary<string, object>
                {
                    {"operateOnTypes", options.OperateOnTypes},
                };
                // todo: send more options here
                uri = UrlHelper.BuildUrl(uri, query);

                var response = await httpClient.PostAsync(uri, content, cancellationToken).ConfigureAwait(false);
                if (response.IsSuccessStatusCode == false)
                    throw new InvalidOperationException("Import failed with status code: " +  response.StatusCode + Environment.NewLine + 
                        await response.Content.ReadAsStringAsync()
                        );

                if (response.IsSuccessStatusCode)
                {
                    var x = await response.Content.ReadAsStringAsync();
                }
            }
        }

        private HttpClient GetHttpClient()
        {
            // TODO: Use HttpClientCache and support api-key
            return new HttpClient
            {
                Timeout = TimeSpan.FromDays(1)
            };
        }

        private void ShowProgress(string message)
        {
            
        }
    }
}