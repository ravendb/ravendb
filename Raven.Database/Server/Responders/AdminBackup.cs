//-----------------------------------------------------------------------
// <copyright file="AdminBackup.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Database.Backup;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class AdminBackup : RequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/admin/backup$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[]{"POST"}; }
		}

		public override void Respond(IHttpContext context)
		{
			if(context.IsAdministrator() == false)
			{
				context.SetStatusToUnauthorized();
				context.WriteJson(new
				{
					Error = "Only administrators can initiate a backup procedure"
				});
			    return;
			}

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
