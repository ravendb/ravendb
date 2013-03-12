using System.Windows.Input;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Tasks
{
	public class ExportTask : TaskModel
	{
		public ExportTask()
		{
			Name = "Export Database";
		    IconResource = "Image_Export_Tiny";
			Description = "Export your database to a dump file. Both indexes and documents are exported.";

			TaskInputs.Add(new TaskCheckBox("Include Documents", true));
			TaskInputs.Add(new TaskCheckBox("Include Indexes", true));
			TaskInputs.Add(new TaskCheckBox("Include Attachments", false));
			TaskInputs.Add(new TaskCheckBox("Include Transformers", true));
		}

		public override ICommand Action
		{
			get { return new ExportDatabaseCommand(this, line => Execute.OnTheUI(() => Output.Add(line))); }
		}
	}
}