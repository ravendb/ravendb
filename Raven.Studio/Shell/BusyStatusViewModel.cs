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
			ActiveTasks = new BindableCollection<string>();
		}

		public bool IsBusy
		{
			get { return count > 0; }
		}

		public IObservableCollection<string> ActiveTasks {get; private set;}

		void IHandle<WorkStarted>.Handle(WorkStarted message)
		{
			var job = message.Job;
			if (!string.IsNullOrEmpty(job)) SimpleLogger.Start(job);
			count++;
			ActiveTasks.Add(job ?? "unknown");
			NotifyOfPropertyChange(() => IsBusy);
		}

		void IHandle<WorkCompleted>.Handle(WorkCompleted message)
		{
			var job = message.Job;
			if(!string.IsNullOrEmpty(job)) SimpleLogger.End(job);
			ActiveTasks.Remove(job ?? "unknown");
			count--;
			NotifyOfPropertyChange( ()=> IsBusy);
		}
	}
}