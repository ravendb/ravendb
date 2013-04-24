using System.Linq;
using Raven.Abstractions.Smuggler;
using Raven.Studio.Features.Input;
using Raven.Studio.Features.Smuggler;
using Raven.Studio.Features.Tasks;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using System;
using System.Windows.Controls;
using TaskStatus = Raven.Studio.Models.TaskStatus;

namespace Raven.Studio.Commands
{
	public class ImportDatabaseCommand : Command
	{
		private readonly Action<string> output;
		private readonly ImportTaskSectionModel taskModel;
		private bool includeAttachments, includeDocuments, includeIndexes, includeTransformers;

		private ISmugglerApi smuggler;

		public ImportDatabaseCommand(ImportTaskSectionModel taskModel, Action<string> output)
		{
			this.output = output;
			this.taskModel = taskModel;
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
			includeAttachments = taskModel.IncludeAttachments.Value;
			includeDocuments = taskModel.IncludeDocuments.Value;
			includeIndexes = taskModel.IncludeIndexes.Value;
			includeTransformers = taskModel.IncludeTransforms.Value;
			
			if (includeDocuments == false && includeAttachments == false && includeIndexes == false && includeTransformers == false)
				return;
			
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

			ItemType operateOnTypes = 0;

			if (includeDocuments)
			{
				operateOnTypes |= ItemType.Documents;
			}

			if (includeAttachments)
			{
				operateOnTypes |= ItemType.Attachments;
			}

			if (includeIndexes)
			{
				operateOnTypes |= ItemType.Indexes;
			}

			if (includeTransformers)
			{
				operateOnTypes |= ItemType.Transformers;
			}

			if (taskModel.UseCollections.Value)
			{
				foreach (var collection in taskModel.Collections.Where(collection => collection.Selected))
				{
					taskModel.Filters.Add(new FilterSetting { Path = "@metadata.Raven-Entity-Name", Value = collection.Name, ShouldMatch = true });
				}
			}

			var smugglerOptions = new SmugglerOptions
			{
				BatchSize = taskModel.Options.Value.BatchSize,
				Filters = taskModel.Filters.ToList(),
				TransformScript = taskModel.ScriptData,
				ShouldExcludeExpired = taskModel.Options.Value.ShouldExcludeExpired,
				OperateOnTypes = operateOnTypes
			};

			smuggler = new SmugglerApi(smugglerOptions, DatabaseCommands, output);

			smuggler.ImportData(stream, smugglerOptions)
			        .Catch(exception => Infrastructure.Execute.OnTheUI(() => taskModel.ReportError(exception)))
			        .Finally(() =>
			        {
				        taskModel.TaskStatus = TaskStatus.Ended;
				        taskModel.CanExecute.Value = true;
			        });
		}
	}
}