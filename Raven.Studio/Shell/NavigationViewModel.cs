using System;
using Raven.Studio.Features.Database;
using Raven.Studio.Framework;

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

		public BindableCollection<IMenuItemMetadata> GoBackMenu
		{
			get
			{
				var itemMetadatas = history
					.Select((historyItem, i)  => (IMenuItemMetadata)new MenuItemMetadata(historyItem.Name , i))
					.Reverse();
				return new BindableCollection<IMenuItemMetadata>(itemMetadatas);
			}
		}

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

			var item = history.Pop();
			item.Reverse();

			NotifyOfPropertyChange(() => CanGoBack);
			NotifyOfPropertyChange(() => GoBackMenu);
		}

		void IHandle<NavigationOccurred>.Handle(NavigationOccurred message)
		{
			history.Push(message);
			if(history.Count > 20) history.Pop();
			NotifyOfPropertyChange(() => CanGoBack);
			NotifyOfPropertyChange(() => GoBackMenu);
		}

		public void GoBackMenuClick(IMenuItemMetadata item)
		{
			NavigationOccurred navigationOccurred = null;
			for (int i = 0; i <= item.Index; i++)
			{
				navigationOccurred = history.Pop();
			}
			if (navigationOccurred != null)
				navigationOccurred.Reverse();

			NotifyOfPropertyChange(() => CanGoBack);
			NotifyOfPropertyChange(() => GoBackMenu);
		}
	}
}