using System.Windows.Input;
using Raven.Abstractions.Data;
using Raven.Studio.Features.Tasks;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class TaskPageModel : PageViewModel
	{
		public TaskPageModel()
        {
            Tasks = new TaskModel();
			ModelUrl = "/Tasks";
        }

        public string CurrentDatabase
        {
            get { return ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name; }
        }

        protected override void OnViewLoaded()
        {
			var databaseName = ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name;

			

	        var import = new ImportTaskSectionModel();
			Tasks.Sections.Add(import);
			Tasks.SelectedSection.Value = import;

			Tasks.Sections.Add(new ExportTaskSectionModel());
			Tasks.Sections.Add(new StartBackupTaskSectionModel());
			
			if (databaseName == Constants.SystemDatabase)
				Tasks.Sections.Add(new StartRestoreTaskSectionModel());

			Tasks.Sections.Add(new IndexingTaskSectionModel());
			Tasks.Sections.Add(new SampleDataTaskSectionModel());
			Tasks.Sections.Add(new CsvImportTaskSectionModel());
        }

        public TaskModel Tasks { get; private set; }
	}
}
