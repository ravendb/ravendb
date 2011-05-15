namespace Raven.Studio.Shell
{
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using Caliburn.Micro;
	using Messages;
	using Action = System.Action;

	[Export]
	public class NavigationViewModel : PropertyChangedBase,
		IHandle<NavigationOccurred>
	{
		readonly IEventAggregator events;
		readonly Stack<NavigationOccurred> history = new Stack<NavigationOccurred>();

		Action goHomeAction;

		[ImportingConstructor]
		public NavigationViewModel(IEventAggregator events)
		{
			this.events = events;
			events.Subscribe(this);
			Breadcrumbs = new BindableCollection<IScreen>();
		}

		public BindableCollection<IScreen> Breadcrumbs { get; private set; }

		public void SetGoHome(Action action)
		{
			goHomeAction = action;
		}

		public void GoHome()
		{
			goHomeAction();
		}

		public bool CanGoBack
		{
			get { return history.Any(); }
		}

		public void GoBack()
		{
			if (CanGoBack == false) return;

			history.Pop().Reverse();

			NotifyOfPropertyChange(() => CanGoBack);
		}

		void IHandle<NavigationOccurred>.Handle(NavigationOccurred message)
		{
			history.Push(message);
			if(history.Count > 20) history.Pop();
			NotifyOfPropertyChange(() => CanGoBack);
		}
	}
}