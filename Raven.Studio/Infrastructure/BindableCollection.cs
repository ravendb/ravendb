using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Windows;
using System.Windows.Threading;
using System.Linq;

namespace Raven.Studio.Infrastructure
{
	public class BindableCollection<T> : ObservableCollection<T>
	{
		private readonly Dispatcher init = Deployment.Current.Dispatcher;

		public void Execute(Action action)
		{
			if (init.CheckAccess())
				action();
			else
				init.InvokeAsync(action);
		}

		public void Match(ICollection<T> items)
		{
			Execute(() =>
			{
				var toRemove = items.Except(this).ToArray();
				var toAdd = this.Except(items).ToArray();

				foreach (var remove in toRemove)
				{
					Remove(remove);
				}

				foreach (var add in toAdd)
				{
					Add(add);
				}
			});
		}

		public void Set(IEnumerable<T> enumerable)
		{
			Execute(() =>
			{
				Clear();
				foreach (var v in enumerable)
				{
					Add(v);
				}
			});
		}
	}
}