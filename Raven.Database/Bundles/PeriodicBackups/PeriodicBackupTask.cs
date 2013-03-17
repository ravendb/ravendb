using System;
using System.ComponentModel.Composition;
using System.IO;
using System.Threading;
using Amazon;
using Amazon.Glacier.Transfer;
using Amazon.S3.Model;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Util;
using Raven.Database.Plugins;
using Raven.Database.Server;
using Raven.Database.Smuggler;
using Raven.Database.Tasks;
using Raven.Json.Linq;
using Task = System.Threading.Tasks.Task;

namespace Raven.Database.Bundles.PeriodicBackups
{
	[InheritedExport(typeof(IStartupTask))]
	[ExportMetadata("Bundle", "PeriodicBackup")]
	public class PeriodicBackupTask : IStartupTask, IDisposable
	{
		public DocumentDatabase Database { get; set; }
		private Timer timer;

		private readonly ILog logger = LogManager.GetCurrentClassLogger();
		private volatile Task currentTask;
		private string awsAccessKey, awsSecretKey;

		private volatile PeriodicBackupSetup backupConfigs;
		private volatile PeriodicBackupStatus backupStatus;

		public void Execute(DocumentDatabase database)
		{
			Database = database;

			Database.OnDocumentChange += (sender, notification, metadata) =>
			{
				if (notification.Id == null)
					return;
				if (PeriodicBackupSetup.RavenDocumentKey.Equals(notification.Id, StringComparison.InvariantCultureIgnoreCase) == false &&
					PeriodicBackupStatus.RavenDocumentKey.Equals(notification.Id, StringComparison.InvariantCultureIgnoreCase) == false)
					return;

				if (timer != null)
					timer.Dispose();

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
					var document = Database.Get(PeriodicBackupSetup.RavenDocumentKey, null);
					if (document == null)
					{
						backupConfigs = null;
						backupStatus = null;
						return;
					}

					var status = Database.Get(PeriodicBackupStatus.RavenDocumentKey, null);

					backupStatus = status == null ? new PeriodicBackupStatus() : status.DataAsJson.JsonDeserialization<PeriodicBackupStatus>();
					backupConfigs = document.DataAsJson.JsonDeserialization<PeriodicBackupSetup>();
					if (backupConfigs.IntervalMilliseconds <= 0)
					{
						logger.Warn("Periodic backup interval is set to zero or less, periodic backup is now disabled");
						return;
					}

					awsAccessKey = Database.Configuration.Settings["Raven/AWSAccessKey"];
					awsSecretKey = Database.Configuration.Settings["Raven/AWSSecretKey"];

					var interval = TimeSpan.FromMilliseconds(backupConfigs.IntervalMilliseconds);
					logger.Info("Periodic backups started, will backup every" + interval.TotalMinutes + "minutes");
					timer = new Timer(TimerCallback, null, TimeSpan.Zero, interval);
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

		private void TimerCallback(object state)
		{
			if (currentTask != null)
				return;

			lock (this)
			{
				if (currentTask != null)
					return;
				currentTask = Task.Factory.StartNew(() =>
				{
					var documentDatabase = Database;
					if (documentDatabase == null)
						return;
					using (LogContext.WithDatabase(documentDatabase.Name))
					{
						try
						{
							var localBackupConfigs = backupConfigs;
							var localBackupStatus = backupStatus;
							if (localBackupConfigs == null)
								return;

							var databaseStatistics = documentDatabase.Statistics;
							// No-op if nothing has changed
							if (databaseStatistics.LastDocEtag == localBackupStatus.LastDocsEtag &&
								databaseStatistics.LastAttachmentEtag == localBackupStatus.LastAttachmentsEtag)
							{
								return;
							}

							var backupPath = localBackupConfigs.LocalFolderName ??
											 Path.Combine(documentDatabase.Configuration.DataDirectory, "PeriodicBackup-Temp");
							var options = new SmugglerOptions
							{
								BackupPath = backupPath,
								LastDocsEtag = localBackupStatus.LastDocsEtag,
								LastAttachmentEtag = localBackupStatus.LastAttachmentsEtag
							};
							var dd = new DataDumper(documentDatabase, options);
							var filePath = dd.ExportData(null, true);

							// No-op if nothing has changed
							if (options.LastDocsEtag == localBackupStatus.LastDocsEtag &&
								options.LastAttachmentEtag == localBackupStatus.LastAttachmentsEtag)
							{
								logger.Info("Periodic backup returned prematurely, nothing has changed since last backup");
								return;
							}

							UploadToServer(filePath, localBackupConfigs);

							localBackupStatus.LastAttachmentsEtag = options.LastAttachmentEtag;
							localBackupStatus.LastDocsEtag = options.LastDocsEtag;

							var ravenJObject = RavenJObject.FromObject(localBackupStatus);
							ravenJObject.Remove("Id");
							var putResult = documentDatabase.Put(PeriodicBackupStatus.RavenDocumentKey, null, ravenJObject,
														 new RavenJObject(), null);

							// this result in backupStatus being refreshed
							localBackupStatus = backupStatus;
							if (localBackupStatus != null)
							{
								if (Etag.Increment(localBackupStatus.LastDocsEtag, 1) == putResult.ETag) // the last etag is with just us
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

		private void UploadToServer(string backupPath, PeriodicBackupSetup localBackupConfigs)
		{
			if (!string.IsNullOrWhiteSpace(localBackupConfigs.GlacierVaultName))
			{
				UploadToGlacier(backupPath, localBackupConfigs);
			}
			else if (!string.IsNullOrWhiteSpace(localBackupConfigs.S3BucketName))
			{
				UploadToS3(backupPath, localBackupConfigs);
			}
		}

		private void UploadToS3(string backupPath, PeriodicBackupSetup localBackupConfigs)
		{
			var awsRegion = RegionEndpoint.GetBySystemName(localBackupConfigs.AwsRegionEndpoint) ?? RegionEndpoint.USEast1;

			using (var client = new Amazon.S3.AmazonS3Client(awsAccessKey, awsSecretKey, awsRegion))
			using (var fileStream = File.OpenRead(backupPath))
			{
				var key = Path.GetFileName(backupPath);
				var request = new PutObjectRequest();
				request.WithMetaData("Description", GetArchiveDescription());
				request.WithInputStream(fileStream);
				request.WithBucketName(localBackupConfigs.S3BucketName);
				request.WithKey(key);

				using (client.PutObject(request))
				{
					logger.Info(string.Format("Successfully uploaded backup {0} to S3 bucket {1}, with key {2}",
											  Path.GetFileName(backupPath), localBackupConfigs.S3BucketName, key));
				}
			}
		}

		private void UploadToGlacier(string backupPath, PeriodicBackupSetup localBackupConfigs)
		{
			var awsRegion = RegionEndpoint.GetBySystemName(localBackupConfigs.AwsRegionEndpoint) ?? RegionEndpoint.USEast1;
			var manager = new ArchiveTransferManager(awsAccessKey, awsSecretKey, awsRegion);
			var archiveId = manager.Upload(localBackupConfigs.GlacierVaultName, GetArchiveDescription(), backupPath).ArchiveId;
			logger.Info(string.Format("Successfully uploaded backup {0} to Glacier, archive ID: {1}", Path.GetFileName(backupPath),
									  archiveId));
		}

		private string GetArchiveDescription()
		{
			return "Periodic backup for db " + (Database.Name ?? Constants.SystemDatabase) + " at " + DateTime.UtcNow;
		}

		public void Dispose()
		{
			if (timer != null)
				timer.Dispose();
			var task = currentTask;
			if (task != null)
				task.Wait();
		}
	}
}
