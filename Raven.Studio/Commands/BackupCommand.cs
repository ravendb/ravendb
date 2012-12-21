using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Studio.Features.Tasks;
using Raven.Studio.Infrastructure;
using System.Linq;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class BackupCommand : Command
	{
		private readonly StartBackupTask startBackupTask;
		public BackupCommand(StartBackupTask startBackupTask)
		{
			this.startBackupTask = startBackupTask;
		}

		public override void Execute(object _)
		{
			var location = startBackupTask.TaskInputs.FirstOrDefault(x => x.Name == "Location");

			if(location == null)
				return;

			ApplicationModel.Current.Server.Value.DocumentStore
				.AsyncDatabaseCommands
				.ForDefaultDatabase()
				.CreateRequest("/admin/databases/" + ApplicationModel.Database.Value.Name, "GET")
				.ReadResponseJsonAsync()
				.ContinueOnSuccessInTheUIThread(doc =>
				{
					if (doc == null)
						return;

					var databaseDocument = ApplicationModel.Current.Server.Value.DocumentStore.Conventions.CreateSerializer()
						.Deserialize<DatabaseDocument>(new RavenJTokenReader(doc));

					DatabaseCommands.StartBackupAsync(location.Value, databaseDocument)
						.ContinueWith(task =>
						{
							task.Wait(); // throws
							startBackupTask.Status = new BackupStatus
							{
								IsRunning = true
							};
						}).Catch(exception => startBackupTask.ReportError(exception));
				});
		}
	}
}