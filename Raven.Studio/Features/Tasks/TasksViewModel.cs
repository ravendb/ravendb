namespace Raven.Studio.Features.Tasks
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using Caliburn.Micro;
	using Framework;
	using Plugins.Database;

	[ExportDatabaseScreen("Tasks", Index = 60)]
	public class TasksViewModel : Conductor<object>,
		IPartImportsSatisfiedNotification
	{
		string selectedTask;

		[ImportingConstructor]
		public TasksViewModel()
		{
			DisplayName = "Tasks";
		}

		[ImportMany("Raven.Task", AllowRecomposition = true)]
		public IEnumerable<Lazy<object, IMenuItemMetadata>> Tasks { get; set; }

		public IList<string> AvailableTasks { get; private set; }

		public string SelectedTask
		{
			get { return selectedTask; }
			set
			{
				selectedTask = value;
				NotifyOfPropertyChange(() => SelectedTask);
				ActivateItem(Tasks.First(x => x.Metadata.DisplayName == selectedTask).Value);
			}
		}

		public void OnImportsSatisfied()
		{
			AvailableTasks = Tasks.Select(x => x.Metadata.DisplayName).ToList();
		}
	}
}