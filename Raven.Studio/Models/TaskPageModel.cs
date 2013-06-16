using System;
using System.Reactive;
using System.Reactive.Linq;
using Raven.Abstractions.Data;
using Raven.Studio.Extensions;
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

		private bool firstLoad = true;

        public string CurrentDatabase
        {
            get { return ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name; }
        }

		private void RegisterToDatabaseChange()
		{
			var databaseChanged = Database.ObservePropertyChanged()
										  .Select(_ => Unit.Default)
										  .TakeUntil(Unloaded);

			databaseChanged
				.Subscribe(_ => OnViewLoaded());
		}

        protected override void OnViewLoaded()
        {
			if (firstLoad)
				RegisterToDatabaseChange();

			firstLoad = false;

			var databaseName = ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name;

			OnPropertyChanged(() => CurrentDatabase);
			Tasks.Sections.Clear();
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
