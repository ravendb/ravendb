using System.Windows.Input;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Tasks
{
	public class StartBackupTask : TaskModel
	{
		public BackupStatus Status { get; set; }

		public StartBackupTask()
		{
			Name = "Backup Database";
			Description = "Backup your database.";
		    IconResource = "Image_Backup_Tiny";
			TaskInputs.Add(new TaskInput("Location", @"C:\path-to-your-backup-folder"));
		}

		public override ICommand Action
		{
			get { return new BackupCommand(this); }
		}

		public override System.Threading.Tasks.Task TimerTickedAsync()
		{
			if (Status == null || Status.IsRunning == false)
				return null;
			TaskStatus = TaskStatus.Started;
			return ApplicationModel.DatabaseCommands.GetAsync(BackupStatus.RavenBackupStatusDocumentKey)
				.ContinueOnSuccessInTheUIThread(item =>
				{
					var documentConvention = ApplicationModel.Current.Server.Value.Conventions;
					Status = documentConvention.CreateSerializer().Deserialize<BackupStatus>(new RavenJTokenReader(item.DataAsJson));

					Output.Clear();
					foreach (var backupMessage in Status.Messages)
					{
						Output.Add("[" + backupMessage.Timestamp + "]   	" + backupMessage.Severity + " :    	"+ backupMessage.Message);	
					}
				})
				.Finally(() => TaskStatus = TaskStatus.Ended);
		}
	}
}