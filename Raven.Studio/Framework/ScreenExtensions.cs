namespace Raven.Studio.Framework
{
	using Caliburn.Micro;
	using Messages;
	using Action = System.Action;

	public static class ScreenExtensions
	{
		public static void TrackNavigationTo(this Conductor<IScreen>.Collection.OneActive screen,
		                                     IScreen newScreen,
		                                     IEventAggregator events)
		{
			TrackNavigationTo(screen, newScreen, events, null);
		}

		public static void TrackNavigationTo(this Conductor<IScreen>.Collection.OneActive screen,
		                                     IScreen newScreen,
		                                     IEventAggregator events,
		                                     Action setContext)
		{
			var old = screen.ActiveItem;
			events.Publish(new NavigationEvent(old.DisplayName, () =>
			                                                    	{
			                                                    		if (setContext == null) setContext();
			                                                    		screen.ActivateItem(old);
			                                                    	}));
			screen.ActivateItem(newScreen);
		}
	}
}