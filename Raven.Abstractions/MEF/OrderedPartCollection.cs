using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Collections.Specialized;

namespace Raven.Abstractions.MEF
{
#if !NET_3_5
	public class OrderedPartCollection<T> : ICollection<Lazy<T, IPartMetadata>>, INotifyCollectionChanged
	{
		private readonly ObservableCollection<Lazy<T, IPartMetadata>> inner = new ObservableCollection<Lazy<T, IPartMetadata>>();

		public IEnumerator<Lazy<T, IPartMetadata>> GetEnumerator()
		{
			return inner.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		public void Add(T item)
		{
			Add(new Lazy<T, IPartMetadata>(() => item, new PartMetadata {Order = 0}));
		}

		public void Add(Lazy<T, IPartMetadata> item)
		{
			int insertAt = 0;
			foreach (var lazy in inner)
			{
				if (item.Metadata.Order > lazy.Metadata.Order)
					break;
				insertAt++;
			}
			inner.Insert(insertAt, item);
		}

		public void Clear()
		{
			inner.Clear();
		}

		public bool Contains(Lazy<T, IPartMetadata> item)
		{
			return inner.Contains(item);
		}

		public void CopyTo(Lazy<T, IPartMetadata>[] array, int arrayIndex)
		{
			inner.CopyTo(array, arrayIndex);
		}

		public bool Remove(Lazy<T, IPartMetadata> item)
		{
			return inner.Remove(item);
		}

		public int Count
		{
			get { return inner.Count; }
		}

		public bool IsReadOnly
		{
			get { return false; }
		}

		public event NotifyCollectionChangedEventHandler CollectionChanged
		{
			add { inner.CollectionChanged += value; }
			remove { inner.CollectionChanged -= value; }
		}
	}
#endif
}