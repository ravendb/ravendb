using Raven.Abstractions.Smuggler;
using Raven.Studio.Features.Smuggler;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Windows.Controls;
using TaskStatus = Raven.Studio.Models.TaskStatus;

namespace Raven.Studio.Commands
{
	public class ExportDatabaseCommand : Command
	{
		const int BatchSize = 512;

		private readonly Action<string> output;
		private Stream stream;
		private readonly TaskModel taskModel;
		private bool includeAttachments, includeDocuments, includeIndexes, includeTransformers;

		private readonly ISmugglerApi smuggler;

		public ExportDatabaseCommand(TaskModel taskModel, Action<string> output)
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
			var attachmentUi = taskModel.TaskInputs.FirstOrDefault(x => x.Name == "Include Attachments") as TaskCheckBox;
			includeAttachments = attachmentUi != null && (bool)attachmentUi.Value;

			var documentsUi = taskModel.TaskInputs.FirstOrDefault(x => x.Name == "Include Documents") as TaskCheckBox;
			includeDocuments = documentsUi != null && (bool)documentsUi.Value;

			var indexesUi = taskModel.TaskInputs.FirstOrDefault(x => x.Name == "Include Indexes") as TaskCheckBox;
			includeIndexes = indexesUi != null && (bool)indexesUi.Value;

			var transformersUi = taskModel.TaskInputs.FirstOrDefault(x => x.Name == "Include Transformers") as TaskCheckBox;
			includeTransformers = transformersUi != null && (bool)transformersUi.Value;

			if (includeDocuments == false && includeAttachments == false && includeIndexes == false && includeTransformers == false)
				return;

			var saveFile = new SaveFileDialog
			{
				DefaultExt = ".ravendump",
				Filter = "Raven Dumps|*.ravendump;*.raven.dump",
			};

			var name = ApplicationModel.Database.Value.Name;
			var normalizedName = new string(name.Select(ch => Path.GetInvalidPathChars().Contains(ch) ? '_' : ch).ToArray());
			var defaultFileName = string.Format("Dump of {0}, {1}", normalizedName, DateTimeOffset.Now.ToString("dd MMM yyyy HH-mm", CultureInfo.InvariantCulture));
			try
			{
				saveFile.DefaultFileName = defaultFileName;
			}
			catch { }

			if (saveFile.ShowDialog() != true)
				return;

			taskModel.TaskStatus = TaskStatus.Started;
			taskModel.CanExecute.Value = false;

			stream = saveFile.OpenFile();

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

			smuggler.ExportData(stream, new SmugglerOptions
			{
				BatchSize = BatchSize,
				OperateOnTypes = operateOnTypes
			}, false)
					.Catch(exception => Infrastructure.Execute.OnTheUI(() =>
					{
						taskModel.ReportError(exception);
						Finish(exception);
					}))
					.Finally(() =>
					{
						taskModel.TaskStatus = TaskStatus.Ended;
						taskModel.CanExecute.Value = true;
					});
		}

		private void Finish(Exception exception)
		{
			stream.Dispose();

			output("Export complete");
			taskModel.CanExecute.Value = true;
			taskModel.TaskStatus = TaskStatus.Ended;
			if (exception != null)
				output(exception.ToString());
		}
	}
}