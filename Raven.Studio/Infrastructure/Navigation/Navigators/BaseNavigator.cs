using System.Collections.Generic;
using System.ComponentModel.Composition;
using Caliburn.Micro;
using Raven.Studio.Messages;
using Raven.Studio.Plugins;

namespace Raven.Studio.Infrastructure.Navigation.Navigators
{
	public abstract class BaseNavigator : INavigator
	{
		[Import]
		public IEventAggregator Events { get; set; }

		[Import]
		public IServer Server { get; set; }

		#region INavigator Members

		public void Navigate(string database, Dictionary<string, string> parameters)
		{
			if (Server.CurrentDatabase == database)
			{
				HandleNavigation(parameters);
				return;
			}

			Server.OpenDatabase(database, () =>
			                              	{
			                              		Events.Publish(new DisplayCurrentDatabaseRequested());
			                              		HandleNavigation(parameters);
			                              	});
		}

		private void HandleNavigation(Dictionary<string, string> parameters)
		{
			var task = string.Format("Navigating to {0}", GetType().Name);
			Events.Publish(new WorkStarted(task));
			OnNavigate(parameters);
			Events.Publish(new WorkCompleted(task));
		}

		#endregion

		protected abstract void OnNavigate(Dictionary<string, string> parameters);
	}
}