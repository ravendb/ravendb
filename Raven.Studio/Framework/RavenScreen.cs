using System.Collections.Generic;
using System.ComponentModel.Composition;
using Raven.Studio.Infrastructure.Navigation;

namespace Raven.Studio.Framework
{
	using Caliburn.Micro;
	using Messages;

	public abstract class RavenScreen : Screen
	{
		protected IEventAggregator Events;
		bool isBusy;

		[Import]
		public NavigationService NavigationService { get; set; }

		protected RavenScreen(IEventAggregator events)
		{
			Events = events;
		}

		protected override void OnViewAttached(object view, object context)
		{
			base.OnViewAttached(view, context);
			NavigationService.Track(GetType(), GetNavigationParameters());
		}

		protected virtual Dictionary<string, string> GetNavigationParameters()
		{
			return null;
		}

		public bool IsBusy
		{
			get { return isBusy; }
			set { isBusy = value; NotifyOfPropertyChange(() => IsBusy); }
		}

		protected void WorkStarted(string job = null)
		{
			//NOTE: this logic isn't entirely consistent. The IsBusy state applies to the screen as a whole 
			// while the work started/completed events could be rasised multiple times by the same screen
			Events.Publish(new WorkStarted(job));
			IsBusy = true;
		}

		protected void WorkCompleted(string job = null)
		{
			Events.Publish(new WorkCompleted(job));
			IsBusy = false;
		}

		protected void NotifyError(string error)
		{
			Events.Publish(new NotificationRaised(error, NotificationLevel.Error));
		}
	}
}