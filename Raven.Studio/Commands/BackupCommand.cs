using System;
using System.Threading.Tasks;
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
		private readonly StartBackupTaskSectionModel startBackupTaskSectionModel;
		public BackupCommand(StartBackupTaskSectionModel startBackupTaskSectionModel)
		{
			this.startBackupTaskSectionModel = startBackupTaskSectionModel;
		}

		protected override async Task ExecuteAsync(object _)
		{
			var location = startBackupTaskSectionModel.TaskInputs.FirstOrDefault(x => x.Name == "Location");

			if (location == null)
				return;

			var asyncDatabaseCommands = ApplicationModel.Current.Server.Value.DocumentStore
			                                            .AsyncDatabaseCommands
			                                            .ForSystemDatabase();

			var httpJsonRequest = asyncDatabaseCommands.CreateRequest(
				"/admin/databases/" + ApplicationModel.Database.Value.Name, "GET");

			var doc = await httpJsonRequest.ReadResponseJsonAsync();
			if (doc == null)
				return;

			var databaseDocument = ApplicationModel.Current.Server.Value.DocumentStore.Conventions.CreateSerializer()
			                                       .Deserialize<DatabaseDocument>(new RavenJTokenReader(doc));

			try
			{
				await DatabaseCommands.StartBackupAsync(location.Value.ToString(), databaseDocument);
				startBackupTaskSectionModel.Status= new BackupStatus
				{
					IsRunning = true
				};
			}
			catch (Exception e)
			{	
					Infrastructure.Execute.OnTheUI(() => startBackupTaskSectionModel.ReportError(e));			
					throw;
			}
		}
	}
}