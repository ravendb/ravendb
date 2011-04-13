namespace Raven.Studio.Framework.Extensions
{
	using System.Collections.Generic;
	using Caliburn.Micro;

	public static class BindableCollectionExtensions
	{
		public static void Replace<T>(this IObservableCollection<T> collection, IEnumerable<T> items)
		{
			collection.IsNotifying = false;
			collection.Clear();
			collection.AddRange(items);
			collection.IsNotifying = true;
			collection.Refresh();
		}
	}
}