namespace Raven.Studio.Framework.Extensions
{
	using Caliburn.Micro;
	using Messages;
	using Action = System.Action;

	public static class ScreenExtensions
	{

		public static void TrackNavigationTo<T>(this T conductor,
											 IScreen newScreen,
											 IEventAggregator events) where T:IHaveActiveItem,IConductor
		{
			TrackNavigationTo(conductor, newScreen, events, null);
		}

		public static void TrackNavigationTo<T>(this T conductor,
											 IScreen newScreen,
											 IEventAggregator events,
											 Action setContext) where T:IHaveActiveItem,IConductor
		{
			var old = conductor.ActiveItem as IScreen;

			if(old != null)
			events.Publish(new NavigationOccurred(old.DisplayName, () =>
			{
				if (setContext != null) setContext();
				conductor.ActivateItem(old);
			}));

			conductor.ActivateItem(newScreen);
		}
	}
}