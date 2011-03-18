//-----------------------------------------------------------------------
// <copyright file="AdminBackup.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Database.Backup;
using Raven.Database.Data;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;

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
			if(context.User.Identity.IsAuthenticated == false ||
			   context.User.IsInRole("Administrators") == false)
			{
				context.SetStatusToForbidden();
				context.WriteJson(new
				{
					Error = "Only administrators can initiate a backup procedure"
				});
			    return;
			}

			var backupRequest = context.ReadJsonObject<BackupRequest>();
			Database.StartBackup(backupRequest.BackupLocation);
			context.SetStatusToCreated(BackupStatus.RavenBackupStatusDocumentKey);
		}
	}
}
