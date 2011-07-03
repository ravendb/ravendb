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

		public void Navigate(Dictionary<string, string> parameters)
		{
			var task = string.Format("Navigating to {0}", GetType().Name);
			Events.Publish(new WorkStarted(task));

			if (parameters.ContainsKey("database"))
			{
				var database = parameters["database"];
				if (Server.CurrentDatabase != database)
					Server.OpenDatabase(database, () => Events.Publish(new DisplayCurrentDatabaseRequested()));
			}

			OnNavigate(parameters);

			Events.Publish(new WorkCompleted(task));
		}

		protected abstract void OnNavigate(Dictionary<string, string> parameters);
	}
}