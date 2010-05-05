using System;
using Raven.Database.Backup;
using Raven.Database.Data;
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
			if(context.User.Identity.IsAuthenticated == false ||
				context.User.IsInRole("Administrators"))
			{
				context.SetStatusToForbidden();
				context.WriteJson(new
				{
					Error = "Only administrators can initiate a backup procedure"
				});
			}

			var backupRequest = context.ReadJsonObject<BackupRequest>();
			Database.StartBackup(backupRequest.BackupLocation);
			context.SetStatusToCreated(BackupStatus.RavenBackupStatusDocumentKey);
		}
	}
}