using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Smuggler;
using Raven.Database.Smuggler;

namespace Raven.Database.Server.Migrator
{
    public class DatabaseMigrator
    {
        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        private readonly DocumentDatabase database;
        private readonly MigrationDetails migrationDetails;
        private readonly int maxIdleTimeForTenantDatabaseInSec;

        public DatabaseMigrator(DocumentDatabase documentDatabase, MigrationDetails migrationDetails, int maxIdleTimeForTenantDatabaseInSec)
        {
            database = documentDatabase;

            if (migrationDetails == null)
                throw new ArgumentException("Migration detatils cannot be null");

            if (string.IsNullOrWhiteSpace(migrationDetails.Url))
                throw new ArgumentException("Migration url cannot be null or empty");


            if (migrationDetails.Url.StartsWith("http://", StringComparison.OrdinalIgnoreCase) == false &&
                migrationDetails.Url.StartsWith("https://", StringComparison.OrdinalIgnoreCase) == false)
            {
                throw new ArgumentException("Url must start with http:// or https://");
            }

            while (migrationDetails.Url.EndsWith("/"))
                migrationDetails.Url =
                    migrationDetails.Url.Substring(0, migrationDetails.Url.Length - 1);

            this.migrationDetails = migrationDetails;
            this.maxIdleTimeForTenantDatabaseInSec = maxIdleTimeForTenantDatabaseInSec;
        }

        public Task Execute(MigrationStatus migrationStatus, CancellationTokenSource cts)
        {
            return Task.Run(async() => await MigrateDatabase(migrationStatus, cts).ConfigureAwait(false), cts.Token);
        }

        private async Task MigrateDatabase(MigrationStatus migrationStatus, CancellationTokenSource cts)
        {
            var url = $"{migrationDetails.Url}/databases/{database.Name}/smuggler/import";
            var request = (HttpWebRequest) WebRequest.Create(url);
            if (string.IsNullOrWhiteSpace(migrationDetails.ClusterToken) == false)
                request.Headers.Add("Raven-Authorization", migrationDetails.ClusterToken);

            request.Method = "POST";
            request.ContentType = "application/octet-stream";
            request.KeepAlive = true;
            request.SendChunked = true;
            request.AllowWriteStreamBuffering = false;
            request.AllowReadStreamBuffering = false;

            var currentMigrationState = migrationDetails.MigrationState;
            try
            {
                var sw = Stopwatch.StartNew();
                using (var outputStream = request.GetRequestStream())
                {
                    var lastCheck = DateTime.MinValue;
                    var smugglerOptions = new SmugglerDatabaseOptions
                    {
                        CancelToken = cts,
                        ExportDeletions = currentMigrationState != null,
                        StartDocsEtag = currentMigrationState?.LastDocumentEtag ?? Etag.Empty,
                        StartDocsDeletionEtag = currentMigrationState?.LastDocumentDeleteEtag ?? Etag.Empty,
                        StartAttachmentsEtag = currentMigrationState?.LastAttachmentEtag ?? Etag.Empty,
                        OperateOnTypes = ItemType.Documents | ItemType.Indexes | ItemType.Transformers | ItemType.Attachments
                    };
                    var dataDumper = new DatabaseDataDumper(database, smugglerOptions);
                    dataDumper.Progress += s =>
                    {
                        migrationStatus.MarkProgress(s);

                        if ((SystemTime.UtcNow - lastCheck).TotalSeconds < maxIdleTimeForTenantDatabaseInSec)
                            return;

                        lastCheck = SystemTime.UtcNow;
                        // prevent the database shutdown
                        database.WorkContext.UpdateFoundWork();
                    };
                    var operationState = await dataDumper.ExportData(
                            new SmugglerExportOptions<RavenConnectionStringOptions>
                            {
                                ToStream = outputStream
                            })
                        .ConfigureAwait(false);

                    var response = (HttpWebResponse) request.GetResponse();
                    migrationStatus.MarkCompleted("Migration completed", sw.Elapsed, operationState);
                    // migration state will be saved to the v4 server by the caller
                }
            }
            catch (ObjectDisposedException)
            {
                // server shutdown
            }
            catch (OperationCanceledException)
            {
                migrationStatus.MarkCanceled();
            }
            catch (Exception e)
            {
                var message = $"Couldn't migrate database {database.Name}";
                migrationStatus.MarkFaulted(message, e);
                Log.ErrorException(message, e);
            }
        }
    }
}