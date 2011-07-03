using Raven.Studio.Infrastructure.Navigation;
using Raven.Studio.Plugins;

namespace Raven.Studio.Framework
{
	using Caliburn.Micro;
	using Messages;

	public abstract class RavenScreen : Screen
	{
		private readonly IServer server;
		protected IEventAggregator Events;
		protected readonly NavigationService NavigationService;
		bool isBusy;

		protected RavenScreen(IServer server, IEventAggregator events, NavigationService navigationService)
		{
			this.server = server;
			Events = events;
			this.NavigationService = navigationService;
		}

		protected override void OnViewAttached(object view, object context)
		{
			base.OnViewAttached(view, context);
			NavigationService.Track(GetScreenNavigationState());
		}

		protected virtual string GetScreenNavigationState()
		{
			return "/" + server.CurrentDatabase + "/" + DisplayName.ToLowerInvariant();
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