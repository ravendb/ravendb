using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Client.Document;

namespace Raven.Client.Smuggler
{
    public class DatabaseSmuggler
    {
        private readonly DocumentStore _store;

        public DatabaseSmuggler(DocumentStore store)
        {
            _store = store;
        }

        public Task ExportAsync(DatabaseSmugglerOptions options, IDatabaseSmugglerDestination destination)
        {
            throw new System.NotImplementedException();
        }

        public async Task ImportIncrementalAsync(DatabaseSmugglerOptions options,  string directoryPath)
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
                await ImportAsync(options, filePath).ConfigureAwait(false);
            }
            options.OperateOnTypes = oldOperateOnTypes;

            var lastFilePath = Path.Combine(directoryPath, files.Last());
            await ImportAsync(options, lastFilePath).ConfigureAwait(false);
        }

        public async Task ImportAsync(DatabaseSmugglerOptions options, string filePath)
        {
            // TODO: Use HttpClientCache and support api-key
            var httpClient = new HttpClient();

            var countOfFileParts = 0;
            do
            {
                ShowProgress($"Starting to import file: {filePath}");
                using (var fileStream = File.OpenRead(filePath))
                {
                    var content = new MultipartFormDataContent($"smuggler-import: {SystemTime.UtcNow:O}");
                    content.Add(new StreamContent(fileStream), Path.GetFileName(filePath), filePath);
                    var database = options.Database ?? _store.DefaultDatabase;
                    await httpClient.PostAsync($"{_store.Url}/databases/{database}/smuggler/import", content).ConfigureAwait(false);
                }
                filePath = $"{filePath}.part{++countOfFileParts:D3}";
            } while (File.Exists(filePath));
        }

        private void ShowProgress(string message)
        {
            
        }
    }
}