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

        public DynamicList(IEnumerable inner)
        {
            this.inner = inner.Cast<object>();
        }

        public DynamicList(IEnumerable<object> inner)
        {
            this.inner = inner;
        }

        internal DynamicList(DynamicJsonObject parent, IEnumerable<object> inner)
            : this(inner)
        {
            this.parent = parent;
        }

        public dynamic Get(params int[] indexes)
        {
            if (indexes == null)
                return new DynamicNullObject();

            dynamic val = this;
            for (int i = 0; i < indexes.Length; i++)
            {
                val = val[indexes[i]];
            }
            return val;
        }

        public override bool TryConvert(ConvertBinder binder, out object result)
        {
            if (binder.ReturnType.IsArray)
            {
                var elementType = binder.ReturnType.GetElementType();
                var count = Count;
                var array = Array.CreateInstance(elementType, count);

                for (int i = 0; i < count; i++)
                {
                    array.SetValue(Convert.ChangeType(this[i], elementType), i);
                }

                result = array;

                return true;
            }
            return base.TryConvert(binder, out result);
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
                {
                    yield return new DynamicJsonObject(parent, ravenJObject);
                    continue;
                }
                var ravenJArray = item as RavenJArray;
                if (ravenJArray != null)
                {
                    yield return new DynamicList(parent, ravenJArray.ToArray());
                    continue;
                }
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

        public int Sum(Func<dynamic, int> selector)
        {
            return Enumerate().Sum(selector);
        }

        public int? Sum(Func<dynamic, int?> selector)
        {
            return inner.Sum(selector) ?? new DynamicNullObject();
        }

        public long Sum(Func<dynamic, long> selector)
        {
            return inner.Sum(selector);
        }

        public long? Sum(Func<dynamic, long?> selector)
        {
            return inner.Sum(selector) ?? new DynamicNullObject();
        }

        public float Sum(Func<dynamic, float> selector)
        {
            return inner.Sum(selector);
        }

        public float? Sum(Func<dynamic, float?> selector)
        {
            return inner.Sum(selector) ?? new DynamicNullObject();
        }

        public double Sum(Func<dynamic, double> selector)
        {
            return inner.Sum(selector);
        }

        public double? Sum(Func<dynamic, double?> selector)
        {
            return inner.Sum(selector) ?? new DynamicNullObject();
        }

        public decimal Sum(Func<dynamic, decimal> selector)
        {
            return inner.Sum(selector);
        }

        public decimal? Sum(Func<dynamic, decimal?> selector)
        {
            return inner.Sum(selector) ?? new DynamicNullObject();
        }

        public dynamic Min()
        {
            return Enumerate().Min() ?? new DynamicNullObject();
        }

        public dynamic Min<TResult>(Func<dynamic, TResult> selector)
        {
            var result = Enumerate().Min(selector);

// ReSharper disable once CompareNonConstrainedGenericWithNull
            if(result == null)
                return new DynamicNullObject();

            return result;
        }

        public dynamic Max()
        {
            return Enumerate().Max() ?? new DynamicNullObject();
        }

        public dynamic Max<TResult>(Func<dynamic, TResult> selector)
        {
            var result = Enumerate().Max(selector);

            // ReSharper disable once CompareNonConstrainedGenericWithNull
            if (result == null)
                return new DynamicNullObject();

            return result;
        }

        public double Average(Func<dynamic, int> selector)
        {
            return inner.Average(selector);
        }

        public double? Average(Func<dynamic, int?> selector)
        {
            return inner.Average(selector) ?? new DynamicNullObject();
        }

        public double Average(Func<dynamic, long> selector)
        {
            return inner.Average(selector);
        }

        public double? Average(Func<dynamic, long?> selector)
        {
            return inner.Average(selector) ?? new DynamicNullObject();
        }

        public float Average(Func<dynamic, float> selector)
        {
            return inner.Average(selector);
        }

        public float? Average(Func<dynamic, float?> selector)
        {
            return inner.Average(selector) ?? new DynamicNullObject();
        }

        public double Average(Func<dynamic, double> selector)
        {
            return inner.Average(selector);
        }

        public double? Average(Func<dynamic, double?> selector)
        {
            return inner.Average(selector) ?? new DynamicNullObject();
        }

        public decimal Average(Func<dynamic, decimal> selector)
        {
            return inner.Average(selector);
        }

        public decimal? Average(Func<dynamic, decimal?> selector)
        {
            return inner.Average(selector) ?? new DynamicNullObject();
        }

        public IEnumerable<dynamic> OrderBy(Func<dynamic, dynamic> comparable)
        {
            return new DynamicList(Enumerate().OrderBy(comparable));
        }

        public IEnumerable<dynamic> OrderByDescending(Func<dynamic, dynamic> comparable)
        {
            return new DynamicList(Enumerate().OrderByDescending(comparable));
        }

        public dynamic GroupBy(Func<dynamic, dynamic> keySelector)
        {
            return new DynamicList(Enumerable.GroupBy(inner, keySelector).Select(x => new WrapperGrouping(x)));
        }

        public dynamic GroupBy(Func<dynamic, dynamic> keySelector, Func<dynamic, dynamic> selector)
        {
            return new DynamicList(Enumerable.GroupBy(inner, keySelector, selector).Select(x => new WrapperGrouping(x)));
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

        public dynamic IndexOf(dynamic item)
        {
            var items = Enumerate().ToList();
            return items.IndexOf(item);
        }

        public dynamic IndexOf(dynamic item, int index)
        {
            var items = Enumerate().ToList();
            return items.IndexOf(item, index);
        }

        public dynamic IndexOf(dynamic item, int index, int count)
        {
            var items = Enumerate().ToList();
            return items.IndexOf(item, index, count);
        }

        public dynamic LastIndexOf(dynamic item)
        {
            var items = Enumerate().ToList();
            return items.LastIndexOf(item);
        }

        public dynamic LastIndexOf(dynamic item, int index)
        {
            var items = Enumerate().ToList();
            return items.LastIndexOf(item, index);
        }

        public dynamic LastIndexOf(dynamic item, int index, int count)
        {
            var items = Enumerate().ToList();
            return items.LastIndexOf(item, index, count);
        }

        public IEnumerable<dynamic> Take(int count)
        {
            return new DynamicList(Enumerate().Take(count));
        }

        public IEnumerable<dynamic> Skip(int count)
        {
            return new DynamicList(Enumerate().Skip(count));
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

        public IEnumerable<object> Select(Func<IGrouping<object,object>, object> func)
        {
            return new DynamicList(parent, inner.Select(o => func((IGrouping<object, object>)o)));
        }

        public IEnumerable<object> Select(Func<object, int, object> func)
        {
            return new DynamicList(parent, inner.Select(func));
        }

        public IEnumerable<object> SelectMany(Func<object, IEnumerable<object>> func)
        {
            return new DynamicList(parent, inner.SelectMany(func));
        }

        public IEnumerable<object> SelectMany(Func<object, IEnumerable<object>> func, Func<object, object, object> selector)
        {
            return new DynamicList(parent, inner.SelectMany(func, selector));
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

        public IEnumerable<dynamic> Except(IEnumerable<dynamic> except)
        {
            return new DynamicList(inner.Except(except));
        }

        public IEnumerable<dynamic> Reverse()
        {
            return new DynamicList(inner.Reverse());
        }

        public bool SequenceEqual(IEnumerable<dynamic> second)
        {
            return Enumerate().SequenceEqual(second);
        }

        public IEnumerable<dynamic> AsEnumerable()
        {
            return this;
        }

        public dynamic[] ToArray()
        {
            return Enumerate().ToArray();
        }

        public List<dynamic> ToList()
        {
            return Enumerate().ToList();
        }

        public Dictionary<TKey, dynamic> ToDictionary<TKey>(Func<dynamic, TKey> keySelector, Func<dynamic, dynamic> elementSelector = null)
        {
            if(elementSelector == null)
                return Enumerate().ToDictionary(keySelector);

            return Enumerate().ToDictionary(keySelector, elementSelector);
        }

        public ILookup<TKey, dynamic> ToLookup<TKey>(Func<dynamic, TKey> keySelector, Func<dynamic, dynamic> elementSelector = null)
        {
            if (elementSelector == null)
                return Enumerate().ToLookup(keySelector);

            return Enumerate().ToLookup(keySelector, elementSelector);
        }

        public IEnumerable<dynamic> OfType<T>()
        {
            return new DynamicList(inner.OfType<T>());
        }

        public IEnumerable<dynamic> Cast<T>()
        {
            return new DynamicList(inner.Cast<T>());
        }

        public dynamic ElementAt(int index)
        {
            return Enumerate().ElementAt(index);
        }

        public dynamic ElementAtOrDefault(int index)
        {
            return Enumerate().ElementAtOrDefault(index) ?? new DynamicNullObject();
        }

        public long LongCount()
        {
            return inner.LongCount();
        }

        public dynamic Aggregate(Func<dynamic, dynamic, dynamic> func)
        {
            return Enumerate().Aggregate(func);
        }

        public dynamic Aggregate(dynamic seed, Func<dynamic, dynamic, dynamic> func)
        {
            return Enumerate().Aggregate((object) seed, func);
        }

        public dynamic Aggregate(dynamic seed, Func<dynamic, dynamic, dynamic> func, Func<dynamic, dynamic> resultSelector)
        {
            return Enumerate().Aggregate((object)seed, func, resultSelector);
        }

        public IEnumerable<dynamic> TakeWhile(Func<dynamic, bool> predicate)
        {
            return new DynamicList(Enumerate().TakeWhile(predicate));
        }

        public IEnumerable<dynamic> TakeWhile(Func<dynamic, int, bool> predicate)
        {
            return new DynamicList(Enumerate().TakeWhile(predicate));
        }

        public IEnumerable<dynamic> SkipWhile(Func<dynamic, bool> predicate)
        {
            return new DynamicList(Enumerate().SkipWhile(predicate));
        }

        public IEnumerable<dynamic> SkipWhile(Func<dynamic, int, bool> predicate)
        {
            return new DynamicList(Enumerate().SkipWhile(predicate));
        }

        public IEnumerable<dynamic> Join(IEnumerable<dynamic> items, Func<dynamic, dynamic> outerKeySelector, Func<dynamic, dynamic> innerKeySelector, 
                                            Func<dynamic, dynamic, dynamic> resultSelector)
        {
            return new DynamicList(Enumerate().Join(items, outerKeySelector, innerKeySelector, resultSelector));
        }

        public IEnumerable<dynamic> GroupJoin(IEnumerable<dynamic> items, Func<dynamic, dynamic> outerKeySelector, Func<dynamic, dynamic> innerKeySelector,
                                            Func<dynamic, dynamic, dynamic> resultSelector)
        {
            return new DynamicList(Enumerate().GroupJoin(items, outerKeySelector, innerKeySelector, resultSelector));
        }

        public IEnumerable<dynamic> Concat(IEnumerable second)
        {
            return new DynamicList(inner.Concat(second.Cast<object>()));
        }

        public IEnumerable<dynamic> Zip(IEnumerable second, Func<dynamic, dynamic, dynamic> resultSelector)
        {
            return new DynamicList(Enumerate().Zip(second.Cast<object>(), resultSelector));
        }

        public IEnumerable<dynamic> Union(IEnumerable second)
        {
            return new DynamicList(inner.Union(second.Cast<object>()));
        }

        public IEnumerable<dynamic> Intersect(IEnumerable second)
        {
            return new DynamicList(inner.Intersect(second.Cast<object>()));
        }
    }

    public class WrapperGrouping : DynamicList, IGrouping<object, object>
    {
        private readonly IGrouping<dynamic, dynamic> inner;

        public WrapperGrouping(IGrouping<dynamic, dynamic> inner)
            : base(inner)
        {
            this.inner = inner;
        }

        public dynamic Key
        {
            get { return inner.Key; }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

}
