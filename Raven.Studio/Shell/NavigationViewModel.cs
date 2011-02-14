namespace Raven.Studio.Shell
{
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Messages;
	using Action = System.Action;

	[Export]
	public class NavigationViewModel : Screen,
		IHandle<NavigationOccurred>
	{
		readonly IEventAggregator events;

		Action goHomeAction;

		[ImportingConstructor]
		public NavigationViewModel(IEventAggregator events)
		{
			this.events = events;
			events.Subscribe(this);
			Breadcrumbs = new BindableCollection<IScreen>();
		}

		public BindableCollection<IScreen> Breadcrumbs {get; private set;}

		public void SetGoHome(Action action)
		{
			goHomeAction = action;
		}

		public void GoHome()
		{
			goHomeAction();
		}

		public void GoBack()
		{
			if(history.Count <= 0) return;

			history.Pop().Reverse();
		}

		readonly Stack<NavigationOccurred> history = new Stack<NavigationOccurred>();

		public void Handle(NavigationOccurred message)
		{
			history.Push(message);
		}
	}
}