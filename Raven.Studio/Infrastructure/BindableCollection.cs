using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace Raven.Studio.Infrastructure
{
	public class BindableCollection<T> : ObservableCollection<T> where T : class
	{
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

		public void Match(ICollection<T> items, Action afterUpdate = null)
		{
			Execute.OnTheUI(() =>
			{
				var toAdd = items.Except(this, objectComparer).ToList();
				var toRemove = this.Except(items, objectComparer).ToArray();
				var toDispose = items.Except(toAdd, objectComparer).OfType<IDisposable>().ToArray();

				for (var i = 0; i < toRemove.Length; i++)
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
				for (var i = 0; i < toAdd.Count; i++)
				{
					var add = toAdd[i];
					Insert(i, add);
				}
				foreach (var disposable in toDispose)
				{
					disposable.Dispose();
				}

				if (afterUpdate != null) afterUpdate();
			});
		}

		private object ExtractKey(T obj)
		{
			return primaryKeyExtractor(obj) ?? obj;
		}

		public void Set(IEnumerable<T> enumerable, Action after = null)
		{
			Execute.OnTheUI(() =>
			{
				Clear();
				foreach (var v in enumerable)
				{
					Add(v);
				}

				if (after != null) after();
			});
		}

		public void AddRange(IEnumerable<T> enumerable)
		{
			Execute.OnTheUI(() =>
			{
				foreach (var v in enumerable)
				{
					Add(v);
				}
			});
		}

		protected override void RemoveItem(int index)
		{
			var disposable = this[index] as IDisposable;
			if (disposable != null)
				disposable.Dispose();

			base.RemoveItem(index);
		}

		protected override void ClearItems()
		{
			foreach (var disposable in this.OfType<IDisposable>())
			{
				disposable.Dispose();
			}

			base.ClearItems();
		}
	}
}