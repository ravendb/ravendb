using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using log4net;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Backup;
using Raven.Database.Json;
using Raven.Database.Extensions;
using Raven.Storage.Managed.Impl;

namespace Raven.Storage.Managed.Backup
{
	public class BackupOperation
	{
		private readonly DocumentDatabase database;
	    private readonly IPersistentSource persistentSource;
	    private readonly string to;
		private readonly string src;

		private readonly ILog log = LogManager.GetLogger(typeof (BackupOperation));

		public BackupOperation(DocumentDatabase database, IPersistentSource persistentSource, string src, string to)
		{
			this.database = database;
		    this.persistentSource = persistentSource;
		    this.to = to.ToFullPath();
            this.src = src.ToFullPath();
		}

		public void Execute(object ignored)
		{
			try
			{
				log.InfoFormat("Starting backup of '{0}' to '{1}'", src, to);
			    var directoryBackups = new List<DirectoryBackup>
			    {
			        new DirectoryBackup(src, to, Path.Combine("TempData" + Guid.NewGuid().ToString("N"))),
			        new DirectoryBackup(Path.Combine(src, "IndexDefinitions"), Path.Combine(to, "IndexDefinitions"),
			                            Path.Combine(src, "Temp" + Guid.NewGuid().ToString("N")))
			    };
				directoryBackups.AddRange(from index in Directory.GetDirectories(Path.Combine(src, "Indexes"))
										  let fromIndex = Path.Combine(src, "Indexes", Path.GetFileName(index))
				                          let toIndex = Path.Combine(to, "Indexes", Path.GetFileName(index))
										  let tempIndex = Path.Combine(src, Path.Combine("BackupTempDirectories",Guid.NewGuid().ToString("N")))
				                          select new DirectoryBackup(fromIndex, toIndex, tempIndex));


                lock (persistentSource.SyncLock)
                {
                    persistentSource.FlushLog();

                    foreach (var directoryBackup in directoryBackups)
                    {
                        directoryBackup.Notify += UpdateBackupStatus;
                        directoryBackup.Prepare();
                    }

                    foreach (var directoryBackup in directoryBackups)
                    {
                        directoryBackup.Execute();
                    }
                }
			}
			catch (Exception e)
			{
				log.Error("Failed to complete backup", e);
				UpdateBackupStatus("Failed to complete backup because: " + e.Message);
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

				var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
				backupStatus.IsRunning = false;
				backupStatus.Completed = DateTime.UtcNow;
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
				var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
				backupStatus.Messages.Add(new BackupStatus.BackupMessage
				{
					Message = newMsg,
					Timestamp = DateTime.UtcNow
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