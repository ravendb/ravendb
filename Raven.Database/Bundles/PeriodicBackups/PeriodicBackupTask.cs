using System;
using System.ComponentModel.Composition;
using System.Globalization;
using System.IO;
using System.Threading;
using Amazon;
using Amazon.Glacier.Transfer;
using Amazon.S3.Model;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Smuggler;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Raven.Database.Server;
using Raven.Database.Smuggler;
using Raven.Json.Linq;
using Task = System.Threading.Tasks.Task;

namespace Raven.Database.Bundles.PeriodicBackups
{
    [InheritedExport(typeof(IStartupTask))]
	[ExportMetadata("Bundle", "PeriodicBackup")]
	public class PeriodicBackupTask : IStartupTask, IDisposable
	{
		public DocumentDatabase Database { get; set; }
        private Timer incrementalBackupTimer;
        private Timer fullBackupTimer;

		private readonly ILog logger = LogManager.GetCurrentClassLogger();
		private volatile Task currentTask;
		private string awsAccessKey, awsSecretKey;
        private string azureStorageAccount, azureStorageKey;

		private volatile PeriodicBackupStatus backupStatus;
		private volatile PeriodicBackupSetup backupConfigs;

		public void Execute(DocumentDatabase database)
		{
			Database = database;

            Database.Notifications.OnDocumentChange += (sender, notification, metadata) =>
			{
				if (notification.Id == null)
					return;
				if (PeriodicBackupSetup.RavenDocumentKey.Equals(notification.Id, StringComparison.InvariantCultureIgnoreCase) == false &&
					PeriodicBackupStatus.RavenDocumentKey.Equals(notification.Id, StringComparison.InvariantCultureIgnoreCase) == false)
					return;

                if (incrementalBackupTimer != null)
                    incrementalBackupTimer.Dispose();

                if (fullBackupTimer != null)
                    fullBackupTimer.Dispose();

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
					// Not having a setup doc means this DB isn't enabled for periodic backups
                    var document = Database.Documents.Get(PeriodicBackupSetup.RavenDocumentKey, null);
					if (document == null)
					{
						backupConfigs = null;
						backupStatus = null;
						return;
					}

                    var status = Database.Documents.Get(PeriodicBackupStatus.RavenDocumentKey, null);

					backupStatus = status == null ? new PeriodicBackupStatus() : status.DataAsJson.JsonDeserialization<PeriodicBackupStatus>();
					backupConfigs = document.DataAsJson.JsonDeserialization<PeriodicBackupSetup>();


					awsAccessKey = Database.Configuration.Settings["Raven/AWSAccessKey"];
					awsSecretKey = Database.Configuration.Settings["Raven/AWSSecretKey"];
                    azureStorageAccount = Database.Configuration.Settings["Raven/AzureStorageAccount"];
                    azureStorageKey = Database.Configuration.Settings["Raven/AzureStorageKey"];

                    if (backupConfigs.IntervalMilliseconds > 0)
                    {
					var interval = TimeSpan.FromMilliseconds(backupConfigs.IntervalMilliseconds);
                        logger.Info("Incremental periodic backups started, will backup every" + interval.TotalMinutes + "minutes");

                        var timeSinceLastBackup = SystemTime.UtcNow - backupStatus.LastBackup;
                        var nextBackup = timeSinceLastBackup >= interval ? TimeSpan.Zero : interval - timeSinceLastBackup;
                        incrementalBackupTimer = new Timer(state => TimerCallback(false), null, nextBackup, interval);
                    }
                    else
                    {
                        logger.Warn("Incremental periodic backup interval is set to zero or less, incremental periodic backup is now disabled");
                    }

                    if (backupConfigs.FullBackupIntervalMilliseconds > 0)
                    {
                        var interval = TimeSpan.FromMilliseconds(backupConfigs.FullBackupIntervalMilliseconds);
                        logger.Info("Full periodic backups started, will backup every" + interval.TotalMinutes + "minutes");

                        var timeSinceLastBackup = SystemTime.UtcNow - backupStatus.LastFullBackup;
					var nextBackup = timeSinceLastBackup >= interval ? TimeSpan.Zero : interval - timeSinceLastBackup;
                        fullBackupTimer = new Timer(state => TimerCallback(true), null, nextBackup, interval);
                    }
                    else
                    {
                        logger.Warn("Full periodic backup interval is set to zero or less, full periodic backup is now disabled");
                    }


				}
				catch (Exception ex)
				{
					logger.ErrorException("Could not read periodic backup config", ex);
					Database.AddAlert(new Alert
					{
						AlertLevel = AlertLevel.Error,
						CreatedAt = SystemTime.UtcNow,
						Message = ex.Message,
						Title = "Could not read periodic backup config",
						Exception = ex.ToString(),
						UniqueKey = "Periodic Backup Config Error"
					});
				}
			}
		}



        private void TimerCallback(bool fullBackup)
		{
			if (currentTask != null)
				return;

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
                            var dataDumper = new DataDumper(documentDatabase);
							var localBackupConfigs = backupConfigs;
							var localBackupStatus = backupStatus;
							if (localBackupConfigs == null)
								return;

                            if (fullBackup == false)
                            {
                                var currentEtags = dataDumper.FetchCurrentMaxEtags();
							// No-op if nothing has changed
                                if (currentEtags.LastDocsEtag == localBackupStatus.LastDocsEtag &&
                                    currentEtags.LastAttachmentsEtag == localBackupStatus.LastAttachmentsEtag &&
                                    currentEtags.LastDocDeleteEtag == localBackupStatus.LastDocsDeletionEtag &&
                                    currentEtags.LastAttachmentsDeleteEtag == localBackupStatus.LastAttachmentDeletionEtag)
							{
								return;
							}
                            }

							var backupPath = localBackupConfigs.LocalFolderName ??
							                 Path.Combine(documentDatabase.Configuration.DataDirectory, "PeriodicBackup-Temp");
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

                            var smugglerOptions = (fullBackup)
                                                      ? new SmugglerOptions()
                                                      : new SmugglerOptions
						    {
                                                          StartDocsEtag = localBackupStatus.LastDocsEtag,
                                                          StartAttachmentsEtag = localBackupStatus.LastAttachmentsEtag,
                                                          StartDocsDeletionEtag = localBackupStatus.LastDocsDeletionEtag,
                                                          StartAttachmentsDeletionEtag = localBackupStatus.LastAttachmentDeletionEtag,
                                                          Incremental = true,
                                                          ExportDeletions = true
                                                      };

                            var exportResult = await dataDumper.ExportData(new SmugglerExportOptions { ToFile = backupPath }, smugglerOptions);

                            if (fullBackup == false)
                                {
						    // No-op if nothing has changed
                                if (exportResult.LastDocsEtag == localBackupStatus.LastDocsEtag &&
                                    exportResult.LastAttachmentsEtag == localBackupStatus.LastAttachmentsEtag &&
                                    exportResult.LastDocDeleteEtag == localBackupStatus.LastDocsDeletionEtag &&
                                    exportResult.LastAttachmentsDeleteEtag == localBackupStatus.LastAttachmentDeletionEtag)
							{
                                    logger.Info(
                                        "Periodic backup returned prematurely, nothing has changed since last backup");
								return;
							}
                            }

							try
							{
                                UploadToServer(exportResult.FilePath, localBackupConfigs, fullBackup);
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
                            var putResult = documentDatabase.Documents.Put(PeriodicBackupStatus.RavenDocumentKey, null, ravenJObject,
								new RavenJObject(), null);

							// this result in backupStatus being refreshed
							localBackupStatus = backupStatus;
							if (localBackupStatus != null)
							{
								if (localBackupStatus.LastDocsEtag.IncrementBy(1) == putResult.ETag) // the last etag is with just us
									localBackupStatus.LastDocsEtag = putResult.ETag; // so we can skip it for the next time
							}
						}
						catch (ObjectDisposedException)
						{
							// shutting down, probably
						}
						catch (Exception e)
						{
							logger.ErrorException("Error when performing periodic backup", e);
							Database.AddAlert(new Alert
							{
								AlertLevel = AlertLevel.Error,
								CreatedAt = SystemTime.UtcNow,
								Message = e.Message,
								Title = "Error in Periodic Backup",
								Exception = e.ToString(),
								UniqueKey = "Periodic Backup Error",
							});
						}
					}
				})
				.ContinueWith(_ =>
				{
					currentTask = null;
				});
			}
		}

        private void UploadToServer(string backupPath, PeriodicBackupSetup localBackupConfigs, bool isFullBackup)
	    {
	        if (!string.IsNullOrWhiteSpace(localBackupConfigs.GlacierVaultName))
	        {
                UploadToGlacier(backupPath, localBackupConfigs, isFullBackup);
	        }
	        else if (!string.IsNullOrWhiteSpace(localBackupConfigs.S3BucketName))
	        {
                UploadToS3(backupPath, localBackupConfigs, isFullBackup);
	        }
	        else if (!string.IsNullOrWhiteSpace(localBackupConfigs.AzureStorageContainer))
	        {
                UploadToAzure(backupPath, localBackupConfigs, isFullBackup);
	        }
	    }

        private void UploadToS3(string backupPath, PeriodicBackupSetup localBackupConfigs, bool isFullBackup)
		{
			var awsRegion = RegionEndpoint.GetBySystemName(localBackupConfigs.AwsRegionEndpoint) ?? RegionEndpoint.USEast1;

			using (var client = new Amazon.S3.AmazonS3Client(awsAccessKey, awsSecretKey, awsRegion))
			using (var fileStream = File.OpenRead(backupPath))
			{
				var key = Path.GetFileName(backupPath);
				var request = new PutObjectRequest();
                request.WithMetaData("Description", GetArchiveDescription(isFullBackup));
				request.WithInputStream(fileStream);
				request.WithBucketName(localBackupConfigs.S3BucketName);
				request.WithKey(key);
                request.WithTimeout(60 * 60 * 1000); // 1 hour
                request.WithReadWriteTimeout(60 * 60 * 1000); // 1 hour

				using (client.PutObject(request))
				{
					logger.Info(string.Format("Successfully uploaded backup {0} to S3 bucket {1}, with key {2}",
											  Path.GetFileName(backupPath), localBackupConfigs.S3BucketName, key));
				}
			}
		}

        private void UploadToGlacier(string backupPath, PeriodicBackupSetup localBackupConfigs, bool isFullBackup)
		{
			var awsRegion = RegionEndpoint.GetBySystemName(localBackupConfigs.AwsRegionEndpoint) ?? RegionEndpoint.USEast1;
			var manager = new ArchiveTransferManager(awsAccessKey, awsSecretKey, awsRegion);
            var archiveId = manager.Upload(localBackupConfigs.GlacierVaultName, GetArchiveDescription(isFullBackup), backupPath).ArchiveId;
			logger.Info(string.Format("Successfully uploaded backup {0} to Glacier, archive ID: {1}", Path.GetFileName(backupPath),
									  archiveId));
		}

        private void UploadToAzure(string backupPath, PeriodicBackupSetup localBackupConfigs, bool isFullBackup)
	    {
            var storageCredentials = new StorageCredentials(azureStorageAccount, azureStorageKey);
            var storageAccount = new CloudStorageAccount(storageCredentials, true);
            var blobClient = new CloudBlobClient(storageAccount.BlobEndpoint, storageCredentials);
            var backupContainer = blobClient.GetContainerReference(localBackupConfigs.AzureStorageContainer);
	        backupContainer.CreateIfNotExists();
	        using (var fileStream = File.OpenRead(backupPath))
	        {
	            var key = Path.GetFileName(backupPath);
                var backupBlob = backupContainer.GetBlockBlobReference(key);
                backupBlob.Metadata.Add("Description", GetArchiveDescription(isFullBackup));
	            backupBlob.UploadFromStream(fileStream);
	            backupBlob.SetMetadata();

                logger.Info(string.Format(
	                "Successfully uploaded backup {0} to Azure container {1}, with key {2}",
	                Path.GetFileName(backupPath),
	                localBackupConfigs.AzureStorageContainer,
	                key));
	        }
	    }

        private string GetArchiveDescription(bool isFullBackup)
		{
            return (isFullBackup ? "Full" : "Incremental") + "periodic backup for db " + (Database.Name ?? Constants.SystemDatabase) + " at " + SystemTime.UtcNow;
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
