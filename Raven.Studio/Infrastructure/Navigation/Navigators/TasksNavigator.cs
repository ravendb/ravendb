using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using Raven.Studio.Features.Collections;
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

			//var activeCollection = tasksViewModel.Collections
			//    .Where(item => item.Name.Equals(collection, StringComparison.InvariantCultureIgnoreCase))
			//    .FirstOrDefault();

			//if (activeCollection == null)
			//    return;

			//collectionsViewModel.ActiveCollection = activeCollection;
		}
	}
}