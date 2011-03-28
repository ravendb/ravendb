namespace Raven.Studio.Shell
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Framework;
	using Messages;

	//TODO: this is admittedly naive, we can make it more robust after all the essential functionality is in place
	[Export]
	[PartCreationPolicy(CreationPolicy.Shared)]
	public class BusyStatusViewModel : PropertyChangedBase,
		IHandle<WorkStarted>,
		IHandle<WorkCompleted>
	{
		int count;

		[ImportingConstructor]
		public BusyStatusViewModel(IEventAggregator events)
		{
			events.Subscribe(this);
		}

		public bool IsBusy
		{
			get { return count > 0; }
		}

		public void Handle(WorkStarted message)
		{
			if (!string.IsNullOrEmpty(message.Job)) SimpleLogger.Start(message.Job);
			count++;
			NotifyOfPropertyChange(() => IsBusy);
		}

		public void Handle(WorkCompleted message)
		{
			if(!string.IsNullOrEmpty(message.Job)) SimpleLogger.End(message.Job);
			count--;
			NotifyOfPropertyChange( ()=> IsBusy);
		}
	}
}