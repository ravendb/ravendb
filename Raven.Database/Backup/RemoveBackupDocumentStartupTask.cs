//-----------------------------------------------------------------------
// <copyright file="RemoveBackupDocumentStartupTask.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Backup
{
	/// <summary>
	/// Delete the backup status document, if it indicate a backup was in progress when the server crashed / shutdown
	/// we have to do that to enable the next backup to complete
	/// </summary>
	public class RemoveBackupDocumentStartupTask : IStartupTask
	{
		public void Execute(DocumentDatabase database)
		{
			var oldBackup = database.Get(BackupStatus.RavenBackupStatusDocumentKey,null);
			if (oldBackup == null)
				return;

			var backupStatus = oldBackup.DataAsJson.JsonDeserialization<BackupStatus>();

			if (backupStatus.Completed != null) // keep the record of the last successful backup
				return;

			database.Delete(BackupStatus.RavenBackupStatusDocumentKey, null, null);
		}
	}
}
