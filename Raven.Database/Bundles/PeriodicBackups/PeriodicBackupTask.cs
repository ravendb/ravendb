using System;
using System.ComponentModel.Composition;
using System.IO;
using System.Threading;
using Amazon;
using Amazon.Glacier.Transfer;
using NLog;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Database.Plugins;
using Raven.Database.Smuggler;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.PeriodicBackups
{
	[InheritedExport(typeof(IStartupTask))]
	[ExportMetadata("Bundle", "PeriodicBackups")]
	public class PeriodicBackupTask : IStartupTask, IDisposable
	{
		public DocumentDatabase Database { get; set; }
		private Timer timer;

		private readonly Logger logger = LogManager.GetCurrentClassLogger();
		private volatile bool executing;

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

			timer = new Timer(TimerCallback, null, TimeSpan.FromMinutes(backupConfigs.Interval), TimeSpan.FromSeconds(backupConfigs.Interval));
			// TODO: enable changing intervals
		}

		private void TimerCallback(object state)
		{
			if (executing)
				return;
			executing = true;

			// Setup doc might be deleted or changed by the user
			var document = Database.Get(PeriodicBackupSetup.RavenDocumentKey, null);
			if (document == null)
			{
				timer.Dispose();
				timer = null;
				return;
			}

			var backupConfigs = document.DataAsJson.JsonDeserialization<PeriodicBackupSetup>();
			if (backupConfigs.Interval <= 0)
			{
				timer.Dispose();
				timer = null;
				return;
			}

			try
			{
				var options = new SmugglerOptions
				{
					BackupPath = Path.GetTempPath(),
					LastDocsEtag = backupConfigs.LastDocsEtag,
					LastAttachmentEtag = backupConfigs.LastAttachmentsEtag
				};
				var dd = new DataDumper(Database, options);
				var filePath = dd.ExportData(null, true);
				var archiveId = UploadToGlacierVault(filePath, backupConfigs);

				logger.Info(string.Format("Successfully uploaded backup {0} to Glacier, archive ID: {1}", Path.GetFileName(filePath), archiveId));

				// Remember the current position only once we are successful, this allows for compensatory backups
				// in case of failures
				backupConfigs.LastAttachmentsEtag = options.LastAttachmentEtag;
				backupConfigs.LastDocsEtag = options.LastDocsEtag;
				Database.Put(PeriodicBackupSetup.RavenDocumentKey, Guid.Empty, RavenJObject.FromObject(backupConfigs),
				             new RavenJObject(), null);
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

		private string UploadToGlacierVault(string backupPath, PeriodicBackupSetup backupConfigs)
		{
			var AWSRegion = RegionEndpoint.USEast1; // TODO make configurable
			var manager = new ArchiveTransferManager(AWSRegion);
			return manager.Upload(backupConfigs.VaultName,
				string.Format("Raven.Database.Backup {0} {1}", Database.Name, DateTimeOffset.UtcNow.ToString("u"))
				, backupPath).ArchiveId;
		}

		public void Dispose()
		{
			if (timer != null)
				timer.Dispose();
		}
	}
}
