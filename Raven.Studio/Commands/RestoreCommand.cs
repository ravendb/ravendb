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
			var location = startRestoreTask.TaskInputs.FirstOrDefault(x => x.Name == "Location");
			var name = startRestoreTask.TaskInputs.FirstOrDefault(x => x.Name == "Database Name");

			if (location == null || name == null)
				return;

			startRestoreTask.TaskStatus = TaskStatus.Started;
			startRestoreTask.CanExecute.Value = false;
			DatabaseCommands.StartRestoreAsync(location.Value, name.Value).Catch();

			UpdateStatus();
		}

		private void UpdateStatus()
		{
			DatabaseCommands.ForDefaultDatabase().GetAsync("Raven/Restore/Status").ContinueOnSuccessInTheUIThread(doc =>
			{
				var status = doc.DataAsJson["restoreStatus"].Values().Select(token => token.ToString()).ToList();
				startRestoreTask.Output.Clear();
				startRestoreTask.Output.AddRange(status);
				if (status.Last().Contains("Complete") == false)
					Time.Delay(TimeSpan.FromMilliseconds(250)).ContinueOnSuccessInTheUIThread(UpdateStatus);
				else
				{
					startRestoreTask.CanExecute.Value = true;
					startRestoreTask.TaskStatus = TaskStatus.Ended;
				}
			});
		}
	}
}