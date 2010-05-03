using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using log4net;
using Microsoft.Isam.Esent.Interop;
using Newtonsoft.Json.Linq;
using Raven.Database.Json;

namespace Raven.Database.Backup
{
	public class BackupOperation
	{
		private readonly JET_INSTANCE instance;
		private readonly DocumentDatabase database;
		private readonly string to;
		private readonly string src;

		private readonly ILog log = LogManager.GetLogger(typeof (BackupOperation));

		public BackupOperation(DocumentDatabase database, string src, string to)
		{
			instance = database.TransactionalStorage.Instance;
			this.database = database;
			this.to = to;
			this.src = src;
		}

		public void Execute()
		{
			try
			{
				log.InfoFormat("Starting backup of '{0}' to '{1}'", src, to);
				var directoryBackups = new List<DirectoryBackup>
				{
					new DirectoryBackup(Path.Combine(src, "IndexDefinitions"), Path.Combine(to, "IndexDefinitions"))
				};
				directoryBackups.AddRange(from index in Directory.GetDirectories(Path.Combine(src, "Indexes"))
										  let fromIndex = Path.Combine(src, "Indexes", Path.GetFileName(index))
				                          let toIndex = Path.Combine(to, "Indexes", Path.GetFileName(index))
				                          select new DirectoryBackup(fromIndex, toIndex));

				foreach (var directoryBackup in directoryBackups)
				{
					directoryBackup.Notify += UpdateBackupStatus;
					directoryBackup.Prepare();
				}

				foreach (var directoryBackup in directoryBackups)
				{
					directoryBackup.Execute();
				}

				var esentBackup = new EsentBackup(instance, to);
				esentBackup.Notify+=UpdateBackupStatus;
				esentBackup.Execute();
			}
			catch (Exception e)
			{
				log.Error("Failed to complete backup", e);
				UpdateBackupStatus("Faile to complete backup because: " + e.Message);
			}
			finally
			{
				CompleteBackup();
			}
		}

		private void CompleteBackup()
		{
			try
			{
				log.Info("Backup completed");
				var jsonDocument = database.Get(BackupStatus.RavenBackupStatusDocumentKey, null);
				if (jsonDocument == null)
					return;

				var backupStatus = jsonDocument.Data.JsonDeserialization<BackupStatus>();
				backupStatus.IsRunning = false;
				backupStatus.Completed = DateTime.Now;
				database.Put(BackupStatus.RavenBackupStatusDocumentKey, null, JObject.FromObject(backupStatus),
				             jsonDocument.Metadata,
				             null);
			}
			catch (Exception e)
			{
				log.Warn("Failed to update completed backup status, will try deleting document", e);
				try
				{
					database.Delete(BackupStatus.RavenBackupStatusDocumentKey, null, null);
				}
				catch (Exception ex)
				{
					log.Warn("Failed to remove out of date backup status", ex);
				}
			}
		}

		private void UpdateBackupStatus(string newMsg)
		{
			try
			{
				log.Info(newMsg);
				var jsonDocument = database.Get(BackupStatus.RavenBackupStatusDocumentKey, null);
				if(jsonDocument==null)
					return;
				var backupStatus = jsonDocument.Data.JsonDeserialization<BackupStatus>();
				backupStatus.Messages.Add(new BackupStatus.BackupMessage
				{
					Message = newMsg,
					Timestamp = DateTime.Now
				});
				database.Put(BackupStatus.RavenBackupStatusDocumentKey, null, JObject.FromObject(backupStatus), jsonDocument.Metadata,
				             null);
			}
			catch (Exception e)
			{
				log.Warn("Failed to update backup status", e);
			}
		}
	}
}