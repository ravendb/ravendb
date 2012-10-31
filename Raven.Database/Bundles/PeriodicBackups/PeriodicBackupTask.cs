using System;
using System.ComponentModel.Composition;
using System.IO;
using System.Threading;
using Amazon;
using Amazon.Glacier.Transfer;
using Amazon.S3.Model;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Smuggler;
using Raven.Database.Plugins;
using Raven.Database.Smuggler;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.PeriodicBackups
{
	[InheritedExport(typeof (IStartupTask))]
	[ExportMetadata("Bundle", "PeriodicBackups")]
	public class PeriodicBackupTask : IStartupTask, IDisposable
	{
		public DocumentDatabase Database { get; set; }
		private Timer timer;
		private int interval;

		private readonly ILog logger = LogManager.GetCurrentClassLogger();
		private volatile bool executing;
		private string awsAccessKey, awsSecretKey;

		public void Execute(DocumentDatabase database)
		{
			Database = database;

			// Not having a setup doc means this DB isn't enabled for periodic backups
			var document = Database.Get(PeriodicBackupSetup.RavenDocumentKey, null);
			if (document == null)
				return;

			var backupConfigs = document.DataAsJson.JsonDeserialization<PeriodicBackupSetup>();
			if (backupConfigs.Interval <= 0)
				return;

			awsAccessKey = Database.Configuration.Settings["Raven/AWSAccessKey"];
			awsSecretKey = Database.Configuration.Settings["Raven/AWSSecretKey"];

			interval = backupConfigs.Interval;
			logger.Info("Periodic backups started, will backup every" + interval + "minutes");
			timer = new Timer(TimerCallback, null, TimeSpan.FromMinutes(interval), TimeSpan.FromMinutes(interval));
		}

		private void TimerCallback(object state)
		{
			if (executing)
				return;
			executing = true;

			PeriodicBackupSetup backupConfigs;
			try
			{
				// Setup doc might be deleted or changed by the user
				var document = Database.Get(PeriodicBackupSetup.RavenDocumentKey, null);
				if (document == null)
				{
					timer.Dispose();
					timer = null;
					return;
				}

				backupConfigs = document.DataAsJson.JsonDeserialization<PeriodicBackupSetup>();
				if (backupConfigs.Interval <= 0)
				{
					timer.Dispose();
					timer = null;
					return;
				}
			}
			catch (Exception ex)
			{
				logger.WarnException(ex.Message, ex);
				executing = false;
				return;
			}

			try
			{
				var options = new SmugglerOptions
				{
					BackupPath = Path.GetTempPath(), //TODO temp path in data folder instead
					LastDocsEtag = backupConfigs.LastDocsEtag,
					LastAttachmentEtag = backupConfigs.LastAttachmentsEtag
				};
				var dd = new DataDumper(Database, options);
				var filePath = dd.ExportData(null, true);
				
				// No-op if nothing has changed
				if (options.LastDocsEtag == backupConfigs.LastDocsEtag && options.LastAttachmentEtag == backupConfigs.LastAttachmentsEtag)
				{
					logger.Info("Periodic backup returned prematurely, nothing has changed since last backup");
					return;
				}

				DoUpload(filePath, backupConfigs);

				// Remember the current position only once we are successful, this allows for compensatory backups
				// in case of failures. We reload the setup document to make sure we don't override changes made by
				// the user.
				// Setup doc might be deleted or changed by the user
				var document = Database.Get(PeriodicBackupSetup.RavenDocumentKey, null);
				if (document == null)
				{
					timer.Dispose();
					timer = null;
					return;
				}

				backupConfigs = document.DataAsJson.JsonDeserialization<PeriodicBackupSetup>();
				backupConfigs.LastAttachmentsEtag = options.LastAttachmentEtag;
				backupConfigs.LastDocsEtag = options.LastDocsEtag;
				Database.Put(PeriodicBackupSetup.RavenDocumentKey, null, RavenJObject.FromObject(backupConfigs),
				             new RavenJObject(), null);

				if (backupConfigs.Interval != interval)
				{
					if (backupConfigs.Interval <= 0)
					{
						timer.Dispose();
						timer = null;
					}
					else
					{
						interval = backupConfigs.Interval;
						timer.Change(TimeSpan.FromMinutes(backupConfigs.Interval), TimeSpan.FromMinutes(backupConfigs.Interval));
					}
				}
			}
			catch (Exception e)
			{
				logger.ErrorException("Error when performing periodic backup", e);
			}
			finally
			{
				executing = false;
			}
		}

		private void DoUpload(string backupPath, PeriodicBackupSetup backupConfigs)
		{
			var AWSRegion = RegionEndpoint.GetBySystemName(backupConfigs.AwsRegionEndpoint) ?? RegionEndpoint.USEast1;

			var desc = string.Format("Raven.Database.Backup {0} {1}", Database.Name,
			                     DateTimeOffset.UtcNow.ToString("u"));

			if (!string.IsNullOrWhiteSpace(backupConfigs.GlacierVaultName))
			{
				var manager = new ArchiveTransferManager(awsAccessKey, awsSecretKey, AWSRegion);
				var archiveId = manager.Upload(backupConfigs.GlacierVaultName, desc, backupPath).ArchiveId;
				logger.Info(string.Format("Successfully uploaded backup {0} to Glacier, archive ID: {1}", Path.GetFileName(backupPath),
										  archiveId));
				return;
			}

			if (!string.IsNullOrWhiteSpace(backupConfigs.S3BucketName))
			{
				var client = new Amazon.S3.AmazonS3Client(awsAccessKey, awsSecretKey, AWSRegion);

				using (var fileStream = File.OpenRead(backupPath))
				{
					var key = Path.GetFileName(backupPath);
					var request = new PutObjectRequest();
					request.WithMetaData("Description", desc);
					request.WithInputStream(fileStream);
					request.WithBucketName(backupConfigs.S3BucketName);
					request.WithKey(key);

					using (S3Response _ = client.PutObject(request))
					{
						logger.Info(string.Format("Successfully uploaded backup {0} to S3 bucket {1}, with key {2}",
							Path.GetFileName(backupPath), backupConfigs.S3BucketName, key));
						return;
					}
				}
			}
		}

	public void Dispose()
		{
			if (timer != null)
				timer.Dispose();
		}
	}
}