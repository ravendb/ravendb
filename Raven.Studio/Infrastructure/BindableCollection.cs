using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Windows;
using System.Windows.Threading;
using System.Linq;

namespace Raven.Studio.Infrastructure
{
	public class BindableCollection<T> : ObservableCollection<T> where T : class
	{
		private readonly Dispatcher init = Deployment.Current.Dispatcher;

		private readonly Func<T, object> primaryKeyExtractor;
		private readonly KeysComparer<T> objectComparer;

		public BindableCollection(Func<T, object> primaryKeyExtractor, KeysComparer<T> objectComparer = null)
		{
			if (objectComparer == null)
				objectComparer = new KeysComparer<T>(primaryKeyExtractor);
			else
				objectComparer.Add(primaryKeyExtractor);
			
			this.primaryKeyExtractor = primaryKeyExtractor;
			this.objectComparer = objectComparer;
		}

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
				var toAdd = items.Except(this, objectComparer).ToList();
				var toRemove = this.Except(items, objectComparer).ToArray();

				for (int i = 0; i < toRemove.Length; i++)
				{
					var remove = toRemove[i];
					var add = toAdd.FirstOrDefault(x => x.Equals(primaryKeyExtractor(remove)));
					if (add == null)
					{
						Remove(remove);
						continue;
					}
					remove = add;
					toAdd.Remove(remove);
				}
				for (int i = 0; i < toAdd.Count; i++)
				{
					var add = toAdd[i];
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