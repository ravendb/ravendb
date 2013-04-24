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
		private readonly StartRestoreTaskSectionModel startRestoreTaskSectionModel;

		public RestoreCommand(StartRestoreTaskSectionModel startRestoreTaskSectionModel)
		{
			this.startRestoreTaskSectionModel = startRestoreTaskSectionModel;
		}

		public override void Execute(object parameter)
		{
			var backupLocation = startRestoreTaskSectionModel.TaskInputs.FirstOrDefault(x => x.Name == "Backup Location");
			var databaseLocation = startRestoreTaskSectionModel.TaskInputs.FirstOrDefault(x => x.Name == "Database Location");
			var name = startRestoreTaskSectionModel.TaskInputs.FirstOrDefault(x => x.Name == "Database Name");
			var attachmentUI = startRestoreTaskSectionModel.TaskInputs.FirstOrDefault(x => x.Name == "Defrag") as TaskCheckBox;
			var defrag = attachmentUI != null && (bool)attachmentUI.Value;
			if (backupLocation == null || name == null || databaseLocation == null)
				return;

			startRestoreTaskSectionModel.TaskStatus = TaskStatus.Started;
			startRestoreTaskSectionModel.CanExecute.Value = false;
			DatabaseCommands.ForSystemDatabase().DeleteDocumentAsync("Raven/Restore/Status")
			                .ContinueOnSuccessInTheUIThread(() =>
			                {
								failCount = 0;
				                DatabaseCommands.StartRestoreAsync(backupLocation.Value.ToString(),
				                                                   databaseLocation.Value.ToString(), name.Value.ToString())
												.ContinueOnSuccess(() => UpdateStatus())
				                                .Catch(exception =>
				                                {
													startRestoreTaskSectionModel.CanExecute.Value = true;
													startRestoreTaskSectionModel.TaskStatus = TaskStatus.Ended;
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
						startRestoreTaskSectionModel.CanExecute.Value = true;
						startRestoreTaskSectionModel.TaskStatus = TaskStatus.Ended;
						startRestoreTaskSectionModel.ReportError("Could not find restore status document, can not know if errors have accrued or if process was completed");
						return;
					}
					else
					{
						Time.Delay(TimeSpan.FromMilliseconds(250)).ContinueOnSuccessInTheUIThread(UpdateStatus);
						return;
					}
				}
				var status = doc.DataAsJson["restoreStatus"].Values().Select(token => token.ToString()).ToList();
				startRestoreTaskSectionModel.Output.Clear();
				startRestoreTaskSectionModel.Output.AddRange(status);
				if (status.Last().Contains("The new database was created") == false && status.Last().Contains("Restore Canceled") == false)
					Time.Delay(TimeSpan.FromMilliseconds(250)).ContinueOnSuccessInTheUIThread(UpdateStatus);
				else
				{
					startRestoreTaskSectionModel.CanExecute.Value = true;
					startRestoreTaskSectionModel.TaskStatus = TaskStatus.Ended;
				}
			})
			.Catch(exception => Infrastructure.Execute.OnTheUI(() =>
			{
				startRestoreTaskSectionModel.ReportError(exception);
				startRestoreTaskSectionModel.CanExecute.Value = true;
				startRestoreTaskSectionModel.TaskStatus = TaskStatus.Ended;
			}));
		}
	}
}