//-----------------------------------------------------------------------
// <copyright file="BackupOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using log4net;
using Raven.Abstractions.Extensions;
using Raven.Database;
using Raven.Database.Backup;
using Raven.Database.Json;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Munin;
using Raven.Storage.Managed.Impl;

namespace Raven.Storage.Managed.Backup
{
	public class BackupOperation
	{
		private readonly DocumentDatabase database;
	    private readonly IPersistentSource persistentSource;
	    private readonly string to;
		private readonly string src;

		private readonly ILog logger = LogManager.GetLogger(typeof (BackupOperation));

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
				logger.InfoFormat("Starting backup of '{0}' to '{1}'", src, to);
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


			    persistentSource.Read(log =>
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

			        return 0;// ignored
			    });
			}
			catch (Exception e)
			{
				logger.Error("Failed to complete backup", e);
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
				logger.Info("Backup completed");
				var jsonDocument = database.Get(BackupStatus.RavenBackupStatusDocumentKey, null);
				if (jsonDocument == null)
					return;

				var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
				backupStatus.IsRunning = false;
				backupStatus.Completed = DateTime.UtcNow;
				database.Put(BackupStatus.RavenBackupStatusDocumentKey, null, RavenJObject.FromObject(backupStatus),
				             jsonDocument.Metadata,
				             null);
			}
			catch (Exception e)
			{
				logger.Warn("Failed to update completed backup status, will try deleting document", e);
				try
				{
					database.Delete(BackupStatus.RavenBackupStatusDocumentKey, null, null);
				}
				catch (Exception ex)
				{
					logger.Warn("Failed to remove out of date backup status", ex);
				}
			}
		}

		private void UpdateBackupStatus(string newMsg)
		{
			try
			{
				logger.Info(newMsg);
				var jsonDocument = database.Get(BackupStatus.RavenBackupStatusDocumentKey, null);
				if(jsonDocument==null)
					return;
				var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
				backupStatus.Messages.Add(new BackupStatus.BackupMessage
				{
					Message = newMsg,
					Timestamp = DateTime.UtcNow
				});
				database.Put(BackupStatus.RavenBackupStatusDocumentKey, null, RavenJObject.FromObject(backupStatus), jsonDocument.Metadata,
				             null);
			}
			catch (Exception e)
			{
				logger.Warn("Failed to update backup status", e);
			}
		}
	}
}