using Raven.Abstractions.Data;
using Raven.Studio.Features.Tasks;
using Raven.Studio.Infrastructure;
using System.Linq;

namespace Raven.Studio.Commands
{
	public class BackupCommand : Command
	{
		private StartBackupTask startBackupTask;
		public BackupCommand(StartBackupTask startBackupTask)
		{
			this.startBackupTask = startBackupTask;
		}

		public override void Execute(object _)
		{
			var location = startBackupTask.TaskInputs.FirstOrDefault(x => x.Name == "Location");

			if(location == null)
				return;

			DatabaseCommands.StartBackupAsync(location.Value)
				.ContinueWith(task =>
				{
					startBackupTask.Status = new BackupStatus
					{
						IsRunning = true
					};
				}).Catch();
		}
	}
}