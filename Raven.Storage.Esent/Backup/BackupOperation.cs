//-----------------------------------------------------------------------
// <copyright file="BackupOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Isam.Esent.Interop;
using NLog;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Database;
using Raven.Database.Backup;
using Raven.Database.Extensions;
using Raven.Json.Linq;

namespace Raven.Storage.Esent.Backup
{
	public class BackupOperation
	{
		private readonly JET_INSTANCE instance;
		private readonly DocumentDatabase database;
		private string to;
		private readonly bool incrementalBackup;
		private string src;

		private static readonly Logger log = LogManager.GetCurrentClassLogger();

		public BackupOperation(DocumentDatabase database, string src, string to, bool incrementalBackup)
		{
			instance = ((TransactionalStorage)database.TransactionalStorage).Instance;
			this.src = src;
			this.to = to;
			this.incrementalBackup = incrementalBackup;
			this.database = database;
			this.src = src;
			this.to = to;
		}

		public void Execute(object ignored)
		{
			try
			{
				to = to.ToFullPath();
				src = src.ToFullPath();
		
				log.Info("Starting backup of '{0}' to '{1}'", src, to);
				var directoryBackups = new List<DirectoryBackup>
				{
					new DirectoryBackup(Path.Combine(src, "IndexDefinitions"), Path.Combine(to, "IndexDefinitions"), Path.Combine(src, "Temp" + Guid.NewGuid().ToString("N")))
				};
				directoryBackups.AddRange(from index in Directory.GetDirectories(database.Configuration.IndexStoragePath)
										  let fromIndex = Path.Combine(database.Configuration.IndexStoragePath, Path.GetFileName(index))
										  let toIndex = Path.Combine(to, "Indexes", Path.GetFileName(index))
										  let tempIndex = Path.Combine(src, Path.Combine("BackupTempDirectories",Guid.NewGuid().ToString("N")))
										  select new DirectoryBackup(fromIndex, toIndex, tempIndex));

				foreach (var directoryBackup in directoryBackups)
				{
					directoryBackup.Notify += UpdateBackupStatus;
					directoryBackup.Prepare();
				}

				foreach (var directoryBackup in directoryBackups)
				{
					directoryBackup.Execute();
				}

				var esentBackup = new EsentBackup(instance, to, incrementalBackup ? BackupGrbit.Incremental : BackupGrbit.Atomic);
				esentBackup.Notify+=UpdateBackupStatus;
				esentBackup.Execute();
			}
			catch (Exception e)
			{
				log.ErrorException("Failed to complete backup", e);
				UpdateBackupStatus("Failed to complete backup because: " + e.Message, BackupStatus.BackupMessageSeverity.Error);
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
				backupStatus.Completed = SystemTime.UtcNow;
				database.Put(BackupStatus.RavenBackupStatusDocumentKey, null, RavenJObject.FromObject(backupStatus),
							 jsonDocument.Metadata,
							 null);
			}
			catch (Exception e)
			{
				log.WarnException("Failed to update completed backup status, will try deleting document", e);
				try
				{
					database.Delete(BackupStatus.RavenBackupStatusDocumentKey, null, null);
				}
				catch (Exception ex)
				{
					log.WarnException("Failed to remove out of date backup status", ex);
				}
			}
		}

		private void UpdateBackupStatus(string newMsg, BackupStatus.BackupMessageSeverity severity)
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
					Timestamp = SystemTime.UtcNow,
					Severity = severity
				});
				database.Put(BackupStatus.RavenBackupStatusDocumentKey, null, RavenJObject.FromObject(backupStatus), jsonDocument.Metadata,
							 null);
			}
			catch (Exception e)
			{
				log.WarnException("Failed to update backup status", e);
			}
		}
	}
}
