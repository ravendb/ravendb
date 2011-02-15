namespace Raven.Studio.Framework
{
	using Caliburn.Micro;
	using Messages;

	public abstract class RavenScreen : Screen
	{
		protected IEventAggregator Events;

		protected RavenScreen(IEventAggregator events)
		{
			Events = events;
		}

		protected void WorkStarted()
		{
			Events.Publish(new WorkStarted());
		}

		protected void WorkCompleted()
		{
			Events.Publish(new WorkCompleted());
		}
	}
}