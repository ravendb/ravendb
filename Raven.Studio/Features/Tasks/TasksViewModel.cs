using Raven.Studio.Common;
using Raven.Studio.Infrastructure.Navigation;

namespace Raven.Studio.Features.Tasks
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using Caliburn.Micro;
	using Framework;
	using Plugins.Database;

	[Export]
	[ExportDatabaseExplorerItem(DisplayName = "Tasks", Index = 60)]
	public class TasksViewModel : Conductor<object>,
		IPartImportsSatisfiedNotification
	{
		private readonly NavigationService navigationService;
		string selectedTask;

		[ImportingConstructor]
		public TasksViewModel(NavigationService navigationService)
		{
			this.navigationService = navigationService;
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
				TrackCurrentTask();
			}
		}

		public void OnImportsSatisfied()
		{
			AvailableTasks = Tasks.Select(x => x.Metadata.DisplayName).ToList();
		}

		private void TrackCurrentTask()
		{
			if (SelectedTask == null)
				return;

			navigationService.Track(new NavigationState
			                        	{
			                        		Url = string.Format("tasks/{0}", SelectedTask.FirstWord().ToLowerInvariant()),
			                        		Title = string.Format("Task: {0}", SelectedTask)
			                        	});
		}

		protected override void OnViewAttached(object view, object context)
		{
			navigationService.Track(new NavigationState {Url = "tasks", Title = "Tasks"});
		}
	}
}