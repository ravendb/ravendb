using Raven.Abstractions.Smuggler;
using Raven.Studio.Features.Input;
using Raven.Studio.Features.Smuggler;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using System;
using System.Windows.Controls;
using TaskStatus = Raven.Studio.Models.TaskStatus;

namespace Raven.Studio.Commands
{
	public class ImportDatabaseCommand : Command
	{
		const int BatchSize = 512;

		private readonly Action<string> output;
		private readonly TaskModel taskModel;

		private readonly ISmugglerApi smuggler;

		public ImportDatabaseCommand(TaskModel taskModel, Action<string> output)
		{
			this.output = output;
			this.taskModel = taskModel;
			smuggler = new SmugglerApi(new SmugglerOptions
			{
				BatchSize = BatchSize
			}, DatabaseCommands, output);
		}

		public override void Execute(object parameter)
		{
			if (ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Statistics.Value != null 
				&& ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Statistics.Value.CountOfDocuments != 0)
			{
				AskUser.ConfirmationWithEvent("Override Documents?", "There are documents in the database :" +
				                                                  ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name +
				                                                  "." + Environment.NewLine
				                                                  + "This operation can override those documents.",
																  ExecuteInternal);
			}
			else
			{
				ExecuteInternal();
			}
		}

		private void ExecuteInternal()
		{
			var openFile = new OpenFileDialog
			{
				Filter = "Raven Dumps|*.ravendump;*.raven.dump",
			};

			if (openFile.ShowDialog() != true)
				return;

			taskModel.TaskStatus = TaskStatus.Started;
			taskModel.CanExecute.Value = false;
			output(String.Format("Importing from {0}", openFile.File.Name));

			var stream = openFile.File.OpenRead();

			smuggler.ImportData(stream, null, incremental: false)
			        .Catch(exception => taskModel.ReportError(exception))
			        .Finally(() =>
			        {
				        taskModel.TaskStatus = TaskStatus.Ended;
				        taskModel.CanExecute.Value = true;
			        });
		}
	}
}