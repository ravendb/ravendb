using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Database.Client.Aws;
using Raven.Database.Client.Azure;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Raven.Database.Smuggler;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.PeriodicExports
{
    [InheritedExport(typeof(IStartupTask))]
    [ExportMetadata("Bundle", "PeriodicExport")]
    public class PeriodicExportTask : IStartupTask, IDisposable
    {
        public DocumentDatabase Database { get; set; }
        private Timer incrementalBackupTimer;
        private Timer fullBackupTimer;

        private readonly ILog logger = LogManager.GetCurrentClassLogger();
        private volatile Task currentTask;
        private string awsAccessKey, awsSecretKey;
        private string azureStorageAccount, azureStorageKey;

        private volatile PeriodicExportStatus exportStatus;
        private volatile PeriodicExportSetup exportConfigs;
        private int backupLimit = int.MaxValue;

        public void Execute(DocumentDatabase database)
        {
            Database = database;

            Database.Notifications.OnDocumentChange += (sender, notification, metadata) =>
            {
                if (notification.Id == null)
                    return;
                if (PeriodicExportSetup.RavenDocumentKey.Equals(notification.Id, StringComparison.InvariantCultureIgnoreCase) == false &&
                    PeriodicExportStatus.RavenDocumentKey.Equals(notification.Id, StringComparison.InvariantCultureIgnoreCase) == false)
                    return;

                if (incrementalBackupTimer != null)
                    Database.TimerManager.ReleaseTimer(incrementalBackupTimer);

                if (fullBackupTimer != null)
                    Database.TimerManager.ReleaseTimer(fullBackupTimer);

                ReadSetupValuesFromDocument();
            };

            ReadSetupValuesFromDocument();
        }

        private void ReadSetupValuesFromDocument()
        {
            using (LogContext.WithDatabase(Database.Name))
            {
                try
                {
                    // Not having a setup doc means this DB isn't enabled for periodic exports
                    var document = Database.Documents.Get(PeriodicExportSetup.RavenDocumentKey, null);
                    if (document == null)
                    {
                        exportConfigs = null;
                        exportStatus = null;
                        return;
                    }

                    var status = Database.Documents.Get(PeriodicExportStatus.RavenDocumentKey, null);

                    exportStatus = status == null ? new PeriodicExportStatus() : status.DataAsJson.JsonDeserialization<PeriodicExportStatus>();
                    exportConfigs = document.DataAsJson.JsonDeserialization<PeriodicExportSetup>();

                    if (exportConfigs.Disabled)
                    {
                        logger.Info("Periodic export is disabled.");
                        return;
                    }

                    awsAccessKey = Database.Configuration.Settings["Raven/AWSAccessKey"];
                    awsSecretKey = Database.Configuration.Settings["Raven/AWSSecretKey"];
                    azureStorageAccount = Database.Configuration.Settings["Raven/AzureStorageAccount"];
                    azureStorageKey = Database.Configuration.Settings["Raven/AzureStorageKey"];

                    if (exportConfigs.IntervalMilliseconds.GetValueOrDefault() > 0)
                    {
                        var interval = TimeSpan.FromMilliseconds(exportConfigs.IntervalMilliseconds.GetValueOrDefault());
                        logger.Info("Incremental periodic export started, will export every" + interval.TotalMinutes + "minutes");

                        var timeSinceLastBackup = SystemTime.UtcNow - exportStatus.LastBackup;
                        var nextBackup = timeSinceLastBackup >= interval ? TimeSpan.Zero : interval - timeSinceLastBackup;

                        incrementalBackupTimer = Database.TimerManager.NewTimer(state => TimerCallback(false), nextBackup, interval);
                    }
                    else
                    {
                        logger.Warn("Incremental periodic export interval is set to zero or less, incremental periodic export is now disabled");
                    }

                    if (exportConfigs.FullBackupIntervalMilliseconds.GetValueOrDefault() > 0)
                    {
                        var interval = TimeSpan.FromMilliseconds(exportConfigs.FullBackupIntervalMilliseconds.GetValueOrDefault());
                        logger.Info("Full periodic export started, will export every" + interval.TotalMinutes + "minutes");

                        var timeSinceLastBackup = SystemTime.UtcNow - exportStatus.LastFullBackup;
                        var nextBackup = timeSinceLastBackup >= interval ? TimeSpan.Zero : interval - timeSinceLastBackup;

                        fullBackupTimer = Database.TimerManager.NewTimer(state => TimerCallback(true), nextBackup, interval);
                    }
                    else
                    {
                        logger.Warn("Full periodic export interval is set to zero or less, full periodic export is now disabled");
                    }
                }
                catch (Exception ex)
                {
                    logger.ErrorException("Could not read periodic export config", ex);
                    Database.AddAlert(new Alert
                    {
                        AlertLevel = AlertLevel.Error,
                        CreatedAt = SystemTime.UtcNow,
                        Message = ex.Message,
                        Title = "Could not read periodic export config",
                        Exception = ex.ToString(),
                        UniqueKey = "Periodic Export Config Error"
                    });
                }
            }
        }
        
        private void TimerCallback(bool fullBackup)
        {
            if (currentTask != null)
                return;

            if (Database.Disposed)
            {
                Dispose();
                return;
            }

            // we have shared lock for both incremental and full backup.
            lock (this)
            {
                if (currentTask != null)
                    return;
                currentTask = Task.Factory.StartNew(async () =>
                {
                    var documentDatabase = Database;
                    if (documentDatabase == null)
                        return;
                    using (LogContext.WithDatabase(documentDatabase.Name))
                    {
                        try
                        {
                            OperationState exportResult;
                            bool performAnotherRun = false;
                            do
                            {
                                var dataDumper = new DatabaseDataDumper(documentDatabase, new SmugglerDatabaseOptions()
                                {
                                    Limit = backupLimit
                                });
                                var localBackupConfigs = exportConfigs;
                                var localBackupStatus = exportStatus;
                                if (localBackupConfigs == null)
                                    return;

                                if (localBackupConfigs.Disabled)
                                    return;

                                if (fullBackup == false)
                                {
                                    var currentEtags = dataDumper.Operations.FetchCurrentMaxEtags();
                                    // No-op if nothing has changed
                                    if (currentEtags.LastDocsEtag == localBackupStatus.LastDocsEtag &&
                                        currentEtags.LastAttachmentsEtag == localBackupStatus.LastAttachmentsEtag &&
                                        currentEtags.LastDocDeleteEtag == localBackupStatus.LastDocsDeletionEtag &&
                                        currentEtags.LastAttachmentsDeleteEtag == localBackupStatus.LastAttachmentDeletionEtag)
                                    {
                                        return;
                                    }
                                }

                                var backupPath = localBackupConfigs.LocalFolderName ?? Path.Combine(documentDatabase.Configuration.DataDirectory, "PeriodicExport-Temp");
                                if (Directory.Exists(backupPath) == false)
                                    Directory.CreateDirectory(backupPath);

                                if (fullBackup)
                                {
                                    // create filename for full dump
                                    backupPath = Path.Combine(backupPath, SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture) + ".ravendb-full-dump");
                                    if (File.Exists(backupPath))
                                    {
                                        var counter = 1;
                                        while (true)
                                        {
                                            backupPath = Path.Combine(Path.GetDirectoryName(backupPath), SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture) + " - " + counter + ".ravendb-full-dump");

                                            if (File.Exists(backupPath) == false)
                                                break;
                                            counter++;
                                        }
                                    }
                                }

                                var smugglerOptions = dataDumper.Options;
                                if (fullBackup == false)
                                {
                                    smugglerOptions.StartDocsEtag = localBackupStatus.LastDocsEtag;
                                    smugglerOptions.StartAttachmentsEtag = localBackupStatus.LastAttachmentsEtag;
                                    smugglerOptions.StartDocsDeletionEtag = localBackupStatus.LastDocsDeletionEtag;
                                    smugglerOptions.StartAttachmentsDeletionEtag = localBackupStatus.LastAttachmentDeletionEtag;
                                    smugglerOptions.Incremental = true;
                                    smugglerOptions.ExportDeletions = true;
                                }

                                exportResult = await dataDumper.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> {ToFile = backupPath});

                                if (fullBackup == false)
                                {
                                    // No-op if nothing has changed
                                    if (exportResult.LastDocsEtag == localBackupStatus.LastDocsEtag &&
                                        exportResult.LastAttachmentsEtag == localBackupStatus.LastAttachmentsEtag &&
                                        exportResult.LastDocDeleteEtag == localBackupStatus.LastDocsDeletionEtag &&
                                        exportResult.LastAttachmentsDeleteEtag == localBackupStatus.LastAttachmentDeletionEtag)
                                    {
                                        logger.Info("Periodic export returned prematurely, nothing has changed since last export");
                                        return;
                                    }
                                }

                                try
                                {
                                    await UploadToServer(exportResult.FilePath, localBackupConfigs, fullBackup);
                                }
                                finally
                                {
                                    // if user did not specify local folder we delete temporary file.
                                    if (String.IsNullOrEmpty(localBackupConfigs.LocalFolderName))
                                    {
                                        IOExtensions.DeleteFile(exportResult.FilePath);
                                    }
                                }

                                if (fullBackup)
                                {
                                    localBackupStatus.LastFullBackup = SystemTime.UtcNow;
                                }
                                else
                                {
                                    localBackupStatus.LastAttachmentsEtag = exportResult.LastAttachmentsEtag;
                                    localBackupStatus.LastDocsEtag = exportResult.LastDocsEtag;
                                    localBackupStatus.LastDocsDeletionEtag = exportResult.LastDocDeleteEtag;
                                    localBackupStatus.LastAttachmentDeletionEtag = exportResult.LastAttachmentsDeleteEtag;
                                    localBackupStatus.LastBackup = SystemTime.UtcNow;
                                }


                                var ravenJObject = JsonExtensions.ToJObject(localBackupStatus);
                                ravenJObject.Remove("Id");
                                var putResult = documentDatabase.Documents.Put(PeriodicExportStatus.RavenDocumentKey, null, ravenJObject,
                                    new RavenJObject(), null);

                                // this result in exportStatus being refreshed
                                localBackupStatus = exportStatus;
                                if (localBackupStatus != null)
                                {
                                    if (localBackupStatus.LastDocsEtag.IncrementBy(1) == putResult.ETag) // the last etag is with just us
                                        localBackupStatus.LastDocsEtag = putResult.ETag; // so we can skip it for the next time
                                }
                                
                                if (backupLimit != int.MaxValue)
                                {
                                    backupLimit = int.MaxValue;
                                    performAnotherRun = true;
                                }
                                else
                                {
                                    performAnotherRun = false;
                                }
                            } while (performAnotherRun);
                        }
                        catch (ObjectDisposedException)
                        {
                            // shutting down, probably
                        }
                        catch (Exception e)
                        {
                            backupLimit  =  100;
                            logger.ErrorException("Error when performing periodic export", e);
                            Database.AddAlert(new Alert
                            {
                                AlertLevel = AlertLevel.Error,
                                CreatedAt = SystemTime.UtcNow,
                                Message = e.Message,
                                Title = "Error in Periodic Export",
                                Exception = e.ToString(),
                                UniqueKey = "Periodic Export Error",
                            });
                        }
                    }
                })
                .Unwrap();

                currentTask.ContinueWith(_ =>
                {
                    currentTask = null;
                });
            }
        }

        private async Task UploadToServer(string backupPath, PeriodicExportSetup localExportConfigs, bool isFullBackup)
        {
            if (!string.IsNullOrWhiteSpace(localExportConfigs.GlacierVaultName))
            {
                UploadToGlacier(backupPath, localExportConfigs, isFullBackup);
            }
            else if (!string.IsNullOrWhiteSpace(localExportConfigs.S3BucketName))
            {
                UploadToS3(backupPath, localExportConfigs, isFullBackup);
            }
            else if (!string.IsNullOrWhiteSpace(localExportConfigs.AzureStorageContainer))
            {
                await UploadToAzure(backupPath, localExportConfigs, isFullBackup);
            }
        }

        private void UploadToS3(string backupPath, PeriodicExportSetup localExportConfigs, bool isFullBackup)
        {
            if (awsAccessKey == Constants.DataCouldNotBeDecrypted ||
                awsSecretKey == Constants.DataCouldNotBeDecrypted)
            {
                throw new InvalidOperationException("Could not decrypt the AWS access settings, if you are running on IIS, make sure that load user profile is set to true.");
            }
            using (var client = new RavenAwsS3Client(awsAccessKey, awsSecretKey, localExportConfigs.AwsRegionEndpoint ?? RavenAwsClient.DefaultRegion))
            using (var fileStream = File.OpenRead(backupPath))
            {
                var key = Path.GetFileName(backupPath);
                client.PutObject(localExportConfigs.S3BucketName, CombinePathAndKey(localExportConfigs.S3RemoteFolderName, key), fileStream, new Dictionary<string, string>
                                                                                   {
                                                                                       { "Description", GetArchiveDescription(isFullBackup) }
                                                                                   }, 60 * 60);

                logger.Info(string.Format("Successfully uploaded backup {0} to S3 bucket {1}, with key {2}",
                                              Path.GetFileName(backupPath), localExportConfigs.S3BucketName, key));
            }
        }

        private void UploadToGlacier(string backupPath, PeriodicExportSetup localExportConfigs, bool isFullBackup)
        {
            if (awsAccessKey == Constants.DataCouldNotBeDecrypted ||
                awsSecretKey == Constants.DataCouldNotBeDecrypted)
            {
                throw new InvalidOperationException("Could not decrypt the AWS access settings, if you are running on IIS, make sure that load user profile is set to true.");
            }
            using (var client = new RavenAwsGlacierClient(awsAccessKey, awsSecretKey, localExportConfigs.AwsRegionEndpoint ?? RavenAwsClient.DefaultRegion))
            using (var fileStream = File.OpenRead(backupPath))
            {
                var archiveId = client.UploadArchive(localExportConfigs.GlacierVaultName, fileStream, GetArchiveDescription(isFullBackup), 60 * 60);
                logger.Info(string.Format("Successfully uploaded backup {0} to Glacier, archive ID: {1}", Path.GetFileName(backupPath), archiveId));
            }
        }

        private async Task UploadToAzure(string backupPath, PeriodicExportSetup localExportConfigs, bool isFullBackup)
        {
            if (azureStorageAccount == Constants.DataCouldNotBeDecrypted ||
                azureStorageKey == Constants.DataCouldNotBeDecrypted)
            {
                throw new InvalidOperationException("Could not decrypt the Azure access settings, if you are running on IIS, make sure that load user profile is set to true.");
            }

            using (var client = new RavenAzureClient(azureStorageAccount, azureStorageKey, localExportConfigs.AzureStorageContainer))
            {
                client.PutContainer();
                using (var fileStream = File.OpenRead(backupPath))
                {
                    var key = Path.GetFileName(backupPath);

                    await client.PutBlob(CombinePathAndKey(localExportConfigs.AzureRemoteFolderName, key), fileStream,
                        new Dictionary<string, string>
                        {
                            {"Description", GetArchiveDescription(isFullBackup)}
                        });

                    logger.Info(string.Format(
                        "Successfully uploaded backup {0} to Azure container {1}, with key {2}",
                        Path.GetFileName(backupPath),
                        localExportConfigs.AzureStorageContainer,
                        key));
                }
            }
        }

        private static string CombinePathAndKey(string path, string fileName)
        {
            return string.IsNullOrEmpty(path) == false ? path + "/" + fileName : fileName;
        }

        private string GetArchiveDescription(bool isFullBackup)
        {
            return (isFullBackup ? "Full" : "Incremental") + " periodic export for db " + (Database.Name ?? Constants.SystemDatabase) + " at " + SystemTime.UtcNow;
        }

        public void Dispose()
        {
            if (incrementalBackupTimer != null)
                incrementalBackupTimer.Dispose();
            if (fullBackupTimer != null)
                fullBackupTimer.Dispose();
            var task = currentTask;
            if (task != null)
                task.Wait();
        }
    }
}
