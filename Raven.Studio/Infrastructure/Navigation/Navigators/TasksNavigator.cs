using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using Raven.Studio.Common;
using Raven.Studio.Features.Tasks;

namespace Raven.Studio.Infrastructure.Navigation.Navigators
{
	[NavigatorExport(@"^tasks/(?<task>.*)", Index = 19)]
	public class TasksNavigator : BaseNavigator
	{
		private readonly TasksViewModel tasksViewModel;

		[ImportingConstructor]
		public TasksNavigator(TasksViewModel tasksViewModel)
		{
			this.tasksViewModel = tasksViewModel;
		}

		protected override void OnNavigate(Dictionary<string, string> parameters)
		{
			var task = parameters["task"];
			if (string.IsNullOrWhiteSpace(task))
				return;

			var activeTask = tasksViewModel.AvailableTasks
				.Where(item => item.FirstWord().Equals(task, StringComparison.InvariantCultureIgnoreCase))
				.FirstOrDefault();

			if (activeTask == null)
				return;

			tasksViewModel.SelectedTask = activeTask;
		}
	}
}