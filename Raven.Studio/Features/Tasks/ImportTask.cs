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
			Description = "Import a database from a dump file.\nImporting will overwrite any existing indexes.";       
		}

		public override ICommand Action
		{
			get { return new ImportDatabaseCommand(line => Execute.OnTheUI(() => Output.Add(line))); }
		}
	}
}