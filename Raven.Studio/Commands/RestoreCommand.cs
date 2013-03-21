using System;
using System.Linq;
using Raven.Client.Extensions;
using Raven.Studio.Features.Tasks;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class RestoreCommand : Command
	{
		private readonly StartRestoreTask startRestoreTask;

		public RestoreCommand(StartRestoreTask startRestoreTask)
		{
			this.startRestoreTask = startRestoreTask;
		}

		public override void Execute(object parameter)
		{
			var backupLocation = startRestoreTask.TaskInputs.FirstOrDefault(x => x.Name == "Backup Location");
			var databaseLocation = startRestoreTask.TaskInputs.FirstOrDefault(x => x.Name == "Database Location");
			var name = startRestoreTask.TaskInputs.FirstOrDefault(x => x.Name == "Database Name");

			if (backupLocation == null || name == null || databaseLocation == null)
				return;

			startRestoreTask.TaskStatus = TaskStatus.Started;
			startRestoreTask.CanExecute.Value = false;
			DatabaseCommands.ForSystemDatabase().DeleteDocumentAsync("Raven/Restore/Status")
			                .ContinueOnSuccessInTheUIThread(() =>
			                {
								failCount = 0;
				                DatabaseCommands.StartRestoreAsync(backupLocation.Value.ToString(),
				                                                   databaseLocation.Value.ToString(), name.Value.ToString())
												.ContinueOnSuccess(() => UpdateStatus())
				                                .Catch(exception =>
				                                {
													startRestoreTask.CanExecute.Value = true;
													startRestoreTask.TaskStatus = TaskStatus.Ended;
				                                });
			                });
		}

		int failCount;
		private void UpdateStatus()
		{
			DatabaseCommands.ForSystemDatabase().GetAsync("Raven/Restore/Status").ContinueOnSuccessInTheUIThread(doc =>
			{
				if (doc == null)
				{
					if (failCount >= 5)
					{
						startRestoreTask.CanExecute.Value = true;
						startRestoreTask.TaskStatus = TaskStatus.Ended;
						startRestoreTask.ReportError("Could not find restore status document, can not know if errors have accrued or if process was completed");
						return;
					}
					else
					{
						Time.Delay(TimeSpan.FromMilliseconds(250)).ContinueOnSuccessInTheUIThread(UpdateStatus);
						return;
					}
				}
				var status = doc.DataAsJson["restoreStatus"].Values().Select(token => token.ToString()).ToList();
				startRestoreTask.Output.Clear();
				startRestoreTask.Output.AddRange(status);
				if (status.Last().Contains("The new database was created") == false && status.Last().Contains("Restore Canceled") == false)
					Time.Delay(TimeSpan.FromMilliseconds(250)).ContinueOnSuccessInTheUIThread(UpdateStatus);
				else
				{
					startRestoreTask.CanExecute.Value = true;
					startRestoreTask.TaskStatus = TaskStatus.Ended;
				}
			})
			.Catch(exception =>
			{
				startRestoreTask.ReportError(exception);
				startRestoreTask.CanExecute.Value = true;
				startRestoreTask.TaskStatus = TaskStatus.Ended;
			});
		}
	}
}