using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using Raven.Json.Linq;

namespace Raven.Abstractions.Linq
{
	public class DynamicList : DynamicObject, IEnumerable<object>
	{
		private readonly DynamicJsonObject parent;
		private readonly IEnumerable<object> inner;

		public DynamicList(IEnumerable<object> inner)
		{
			this.inner = inner;
		}

		internal DynamicList(DynamicJsonObject parent, IEnumerable<object> inner)
			: this(inner)
		{
			this.parent = parent;
		}

		public override bool TryInvokeMember(InvokeMemberBinder binder, object[] args, out object result)
		{
			switch (binder.Name)
			{
				case "AsEnumerable":
					result = this;
					return true;
				case "Count":
					if (args.Length == 0)
					{
						result = Count;
						return true;
					}
					result = Enumerable.Count(this, (Func<object, bool>)args[0]);
					return true;
				case "DefaultIfEmpty":
					result = inner.DefaultIfEmpty(new DynamicNullObject());
					return true;
			}
			return base.TryInvokeMember(binder, args, out result);
		}

		private IEnumerable<dynamic> Enumerate()
		{
			foreach (var item in inner)
			{
				var ravenJObject = item as RavenJObject;
				if (ravenJObject != null)
					yield return new DynamicJsonObject(parent, ravenJObject);
				var ravenJArray = item as RavenJArray;
				if (ravenJArray != null)
					yield return new DynamicList(parent, ravenJArray.ToArray());
				yield return item;
			}
		}

		public dynamic First()
		{
			return Enumerate().First();
		}

		public dynamic First(Func<dynamic, bool> predicate)
		{
			return Enumerate().First(predicate);
		}

		public dynamic Any(Func<dynamic, bool> predicate)
		{
			return Enumerate().Any(predicate);
		}

		public dynamic All(Func<dynamic, bool> predicate)
		{
			return Enumerate().All(predicate);
		}

		public dynamic FirstOrDefault(Func<dynamic, bool> predicate)
		{
			return Enumerate().FirstOrDefault(predicate) ?? new DynamicNullObject();
		}

		public dynamic FirstOrDefault()
		{
			return Enumerate().FirstOrDefault() ?? new DynamicNullObject();
		}

		public dynamic Single(Func<dynamic, bool> predicate)
		{
			return Enumerate().Single(predicate);
		}

		public IEnumerable<dynamic> Distinct()
		{
			return new DynamicList(Enumerate().Distinct().ToArray());
		}

		public dynamic SingleOrDefault(Func<dynamic, bool> predicate)
		{
			return Enumerate().SingleOrDefault(predicate) ?? new DynamicNullObject();
		}

		public dynamic SingleOrDefault()
		{
			return Enumerate().SingleOrDefault() ?? new DynamicNullObject();
		}

		public IEnumerator<object> GetEnumerator()
		{
			return Enumerate().GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return Enumerate().GetEnumerator();
		}


		public void CopyTo(Array array, int index)
		{
			((ICollection)inner).CopyTo(array, index);
		}

		public object this[int index]
		{
			get { return inner.ElementAt(index); }
		}

		public bool Contains(object item)
		{
			return inner.Contains(item);
		}

		public int Count
		{
			get { return inner.Count(); }
		}

		public int Sum(Func<dynamic, int> aggregator)
		{
			return Enumerate().Sum(aggregator);
		}

		public decimal Sum(Func<dynamic, decimal> aggregator)
		{
			return Enumerate().Sum(aggregator);
		}

		public float Sum(Func<dynamic, float> aggregator)
		{
			return Enumerate().Sum(aggregator);
		}

		public double Sum(Func<dynamic, double> aggregator)
		{
			return Enumerate().Sum(aggregator);
		}

		public long Sum(Func<dynamic, long> aggregator)
		{
			return Enumerate().Sum(aggregator);
		}

		public IEnumerable<dynamic> OrderBy(Func<dynamic, dynamic> comparable)
		{
			return new DynamicList(Enumerate().OrderBy(comparable));
		}

		public IEnumerable<dynamic> OrderByDescending(Func<dynamic, dynamic> comparable)
		{
			return new DynamicList(Enumerate().OrderByDescending(comparable));
		}

		public dynamic Last()
		{
			return Enumerate().Last();
		}

		public dynamic LastOrDefault()
		{
			return Enumerate().LastOrDefault() ?? new DynamicNullObject();
		}

		public dynamic Last(Func<dynamic, bool> predicate)
		{
			return Enumerate().Last(predicate);
		}

		public dynamic LastOrDefault(Func<dynamic, bool> predicate)
		{
			return Enumerate().LastOrDefault(predicate) ?? new DynamicNullObject();
		}

		/// <summary>
		/// Gets the length.
		/// </summary>
		/// <value>The length.</value>
		public int Length
		{
			get { return inner.Count(); }
		}

		public IEnumerable<object> Select(Func<object, object> func)
		{
			return new DynamicList(parent, inner.Select(func));
		}

		public IEnumerable<object> Select(Func<object, int, object> func)
		{
			return new DynamicList(parent, inner.Select(func));
		}

		public IEnumerable<object> SelectMany(Func<object, IEnumerable<object>> func)
		{
			return new DynamicList(parent, inner.SelectMany(func));
		}

		public IEnumerable<object> SelectMany(Func<object, int, IEnumerable<object>> func)
		{
			return new DynamicList(parent, inner.SelectMany(func));
		}

		public IEnumerable<object> Where(Func<object, bool> func)
		{
			return new DynamicList(parent, inner.Where(func));
		}

		public IEnumerable<object> Where(Func<object, int, bool> func)
		{
			return new DynamicList(parent, inner.Where(func));
		}

		public dynamic DefaultIfEmpty(object defaultValue = null)
		{
			return inner.DefaultIfEmpty(defaultValue ?? new DynamicNullObject());
		}
	}
}