using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Windows;
using System.Windows.Threading;
using System.Linq;
using Raven.Abstractions.Data;

namespace Raven.Studio.Infrastructure
{
	public class BindableCollection<T> : ObservableCollection<T> where T : class
	{
		private readonly Dispatcher init = Deployment.Current.Dispatcher;

		private readonly Func<T, object> primaryKeyExtractor;
		private readonly KeysComparer<T> objectComparer;

	    public event EventHandler<EventArgs> Updated;

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

		public void Match(ICollection<T> items, Action afterUpdate = null)
		{
			Execute(() =>
			{
				var toAdd = items.Except(this, objectComparer).ToList();
				var toRemove = this.Except(items, objectComparer).ToArray();

				for (int i = 0; i < toRemove.Length; i++)
				{
					var remove = toRemove[i];
					var add = toAdd.FirstOrDefault(x => Equals(ExtractKey(x), ExtractKey(remove)));
					if (add == null)
					{
						Remove(remove);
						continue;
					}
					SetItem(Items.IndexOf(remove), add);
					toAdd.Remove(add);
				}
				foreach (var add in toAdd)
				{
					Add(add);
				}

				if (afterUpdate != null) afterUpdate();

                RaiseUpdated();
			});

			if (afterUpdate != null) afterUpdate();
		}

		private object ExtractKey(T obj)
		{
			return primaryKeyExtractor(obj) ?? obj;
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

                RaiseUpdated();
			});
		}

		public void AddRange(IEnumerable<T> enumerable)
		{
			Execute(() =>
			{
				foreach (var v in enumerable)
				{
					Add(v);
				}

                RaiseUpdated();
			});
		}

        private void RaiseUpdated()
        {
            var handler = Updated;
            if (handler != null)
            {
                handler(this, EventArgs.Empty);
            }
        }
	}
}