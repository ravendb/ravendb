using System.Windows.Input;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Tasks
{
	public class ExportTaskSectionModel : SmugglerTaskSectionModel
	{
		public ExportTaskSectionModel()
		{
			Name = "Export Database";
		    IconResource = "Image_Export_Tiny";
			Description = "Export your database to a dump file. Both indexes and documents are exported.";
		}

		public override ICommand Action
		{
			get { return new ExportDatabaseCommand(this, line => Execute.OnTheUI(() => Output.Add(line))); }
		}
	}
}