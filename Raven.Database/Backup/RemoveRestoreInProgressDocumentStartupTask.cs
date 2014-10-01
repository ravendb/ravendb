//-----------------------------------------------------------------------
// <copyright file="RemoveBackupDocumentStartupTask.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Backup
{
	/// <summary>
	/// Delete the restore in progress document, if it indicate a restore was in progress when the server crashed / shutdown
	/// we have to do that to enable the next restore to complete
	/// </summary>
	public class RemoveRestoreInProgressDocumentStartupTask : IStartupTask
	{
		public void Execute(DocumentDatabase database)
		{
			var oldBackup = database.Documents.Get(RestoreInProgress.RavenRestoreInProgressDocumentKey,null);
			if (oldBackup == null)
				return;

            database.Documents.Delete(RestoreInProgress.RavenRestoreInProgressDocumentKey, null, null);
		}
	}
}
