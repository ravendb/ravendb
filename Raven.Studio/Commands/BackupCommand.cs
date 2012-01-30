using Raven.Abstractions.Data;
using Raven.Studio.Features.Tasks;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Commands
{
	public class BackupCommand : Command
	{
		private StartBackupTask startBackupTask;
		public BackupCommand(StartBackupTask startBackupTask)
		{
			this.startBackupTask = startBackupTask;
		}

		public override void Execute(object parameter)
		{
			var location = parameter as string;
			if(location == null)
				return;

			DatabaseCommands.StartBackupAsync(location)
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