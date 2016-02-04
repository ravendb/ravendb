using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Abstractions.Util;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace Raven.Smuggler.Database.Remote
{
    public class DatabaseSmugglerRemoteDocumentActions : IDatabaseSmugglerDocumentActions
    {
        private readonly DatabaseSmugglerRemoteDestinationOptions _options;

        private readonly DatabaseSmugglerNotifications _notifications;

        private readonly BulkInsertOperation _bulkInsert;

        private readonly Stopwatch _timeSinceLastWrite;

        private readonly object _locker = new object();

        public DatabaseSmugglerRemoteDocumentActions(DatabaseSmugglerOptions globalOptions, DatabaseSmugglerRemoteDestinationOptions options, DatabaseSmugglerNotifications notifications, DocumentStore store)
        {
            _options = options;
            _notifications = notifications;
            _bulkInsert = store.BulkInsert(store.DefaultDatabase, new BulkInsertOptions
            {
                BatchSize = globalOptions.BatchSize,
                OverwriteExisting = true,
                Compression = options.DisableCompression ? BulkInsertCompression.None : BulkInsertCompression.GZip,
                ChunkedBulkInsertOptions = new ChunkedBulkInsertOptions
                {
                    MaxChunkVolumeInBytes = options.TotalDocumentSizeInChunkLimitInBytes,
                    MaxDocumentsPerChunk = options.ChunkSize
                }
            });

            _notifications.OnDocumentRead += DocumentFound;
            _timeSinceLastWrite = Stopwatch.StartNew();
        }

        public void Dispose()
        {
            _notifications.OnDocumentRead -= DocumentFound;
            _bulkInsert?.Dispose();
        }

        public Task WriteDocumentAsync(RavenJObject document, CancellationToken cancellationToken)
        {
            var metadata = document.Value<RavenJObject>("@metadata");
            document.Remove("@metadata");

            var id = metadata.Value<string>("@id");

            _bulkInsert.Store(document, metadata, id);
            return new CompletedTask();
        }

        private async void DocumentFound(object sender, string key)
        {
            if (_timeSinceLastWrite.Elapsed <= _options.HeartbeatLatency)
                return;

            Monitor.Enter(_locker);
            try
            {
                if (_timeSinceLastWrite.Elapsed <= _options.HeartbeatLatency)
                    return;

                var buildSkipDocument = BuildSkipDocument();
                await WriteDocumentAsync(buildSkipDocument, CancellationToken.None).ConfigureAwait(false);
                _timeSinceLastWrite.Restart();
            }
            finally
            {
                Monitor.Exit(_locker);
            }
        }

        private static RavenJObject BuildSkipDocument()
        {
            var metadata = new RavenJObject();
            metadata.Add("@id", Constants.BulkImportHeartbeatDocKey);
            var skipDoc = new JsonDocument
            {
                Key = Constants.BulkImportHeartbeatDocKey,
                DataAsJson = RavenJObject.FromObject(new
                {
                    LastHearbeatSent = SystemTime.UtcNow
                }),
                Metadata = metadata
            };

            return skipDoc.ToJson();
        }
    }
}
