using System.ComponentModel.Composition;
using Raven.Studio.Infrastructure.Navigation;
using Raven.Studio.Plugins;

namespace Raven.Studio.Framework
{
	using Caliburn.Micro;
	using Messages;

	public abstract class RavenScreen : Screen
	{
		bool isBusy;

		[Import]
		public IServer Server { get; set; }
		[Import]
		public IEventAggregator Events { get; set; }
		[Import]
		public NavigationService NavigationService { get; set; }

		protected override void OnViewAttached(object view, object context)
		{
			base.OnViewAttached(view, context);
			NavigationService.Track(GetScreenNavigationState());
		}

		protected virtual string GetScreenNavigationState()
		{
			return "/" + Server.CurrentDatabase + "/" + DisplayName.ToLowerInvariant();
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