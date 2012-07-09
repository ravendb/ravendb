//-----------------------------------------------------------------------
// <copyright file="AdminBackup.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Abstractions.Data;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders.Admin
{
	public class AdminBackup : AdminResponder
	{
		public override void RespondToAdmin(IHttpContext context)
		{
			var backupRequest = context.ReadJsonObject<BackupRequest>();
			var incrementalString = context.Request.QueryString["incremental"];
			bool incrementalBackup;
			if (bool.TryParse(incrementalString, out incrementalBackup) == false)
				incrementalBackup = false;
			Database.StartBackup(backupRequest.BackupLocation, incrementalBackup);
			context.SetStatusToCreated(BackupStatus.RavenBackupStatusDocumentKey);
		}
	}
}
