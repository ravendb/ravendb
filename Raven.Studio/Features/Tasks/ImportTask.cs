using System.Windows.Input;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Tasks
{
	public class ImportTask : TaskModel
	{
		public ImportTask()
		{
			Name = "Import Database";
            IconResource = "Image_Import_Tiny";
			Description = "Import data to the current database.\nImporting will overwrite any existing indexes.";       
		}

		public override ICommand Action
		{
			get { return new ImportDatabaseCommand(this, line => Execute.OnTheUI(() => Output.Add(line))); }
		}
	}
}