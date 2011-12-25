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
			Description = "Export your database to a dump file. By default, both indexes and documents are exported.\nYou can optionally choose to export just indexes.";
		}

		public override ICommand Action
		{
			get { return new ExportDatabaseCommand(line => Execute.OnTheUI(() => Output.Add(line))); }
		}
	}
}