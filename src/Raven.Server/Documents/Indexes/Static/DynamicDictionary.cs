using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Documents.Indexes.Static
{
    public class DynamicDictionary : IDictionary<object, object>, IOrderedEnumerable<object>
    {
        private readonly Dictionary<object, object> _dictionary;

        public DynamicDictionary(Dictionary<object, object> dictionary)
        {
            _dictionary = dictionary;
        }

        public DynamicDictionary(IEnumerable<KeyValuePair<object, object>> dictionary)
        {
            _dictionary = (Dictionary<object, object>)dictionary;
        }

        IEnumerator<object> IEnumerable<object>.GetEnumerator()
        {
            foreach (var kvp in _dictionary)
            {
                yield return kvp;
            }
        }

        public IEnumerator<KeyValuePair<object, object>> GetEnumerator()
        {
            foreach (var kvp in _dictionary)
            {
                yield return kvp;
            }
        }

        public IOrderedEnumerable<object> CreateOrderedEnumerable<TKey>(Func<object, TKey> keySelector, IComparer<TKey> comparer, bool descending)
        {
            throw new NotSupportedException();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;

            if (ReferenceEquals(this, obj))
                return true;

            if (obj is DynamicDictionary dDictionary)
                return Equals(_dictionary, dDictionary._dictionary);

            return Equals(_dictionary, obj);
        }

        public override int GetHashCode()
        {
            return _dictionary?.GetHashCode() ?? 0;
        }

        public dynamic Aggregate(Func<dynamic, dynamic, dynamic> func)
        {
            return Enumerable.Aggregate(this, func);
        }

        public dynamic Aggregate(dynamic seed, Func<dynamic, dynamic, dynamic> func)
        {
            return Enumerable.Aggregate(this, (object)seed, func);
        }

        public dynamic Aggregate(dynamic seed, Func<dynamic, dynamic, dynamic> func, Func<dynamic, dynamic> resultSelector)
        {
            return Enumerable.Aggregate(this, (object)seed, func, resultSelector);
        }

        public void Add(KeyValuePair<object, object> item)
        {
            throw new NotSupportedException();
        }

        public void Clear()
        {
            throw new NotSupportedException();
        }

        public bool Contains(KeyValuePair<object, object> item)
        {
            return _dictionary.Contains(item);
        }

        public void CopyTo(KeyValuePair<object, object>[] array, int arrayIndex)
        {
            throw new NotSupportedException();
        }

        public bool Remove(KeyValuePair<object, object> item)
        {
            return _dictionary.Remove(item);
        }

        public int Count => _dictionary.Count();
        public bool IsReadOnly => throw new NotSupportedException();

        public void Add(object key, object value)
        {
            throw new NotSupportedException();
        }

        public bool ContainsKey(object key)
        {
            return _dictionary.ContainsKey(key);
        }

        public bool Remove(object key)
        {
            return _dictionary.Remove(key);
        }

        public bool TryGetValue(object key, out object value)
        {
            return _dictionary.TryGetValue(key, out value);
        }

        public object this[object key]
        {
            get => _dictionary[key];
            set => throw new NotSupportedException();
        }

        public ICollection<object> Keys => _dictionary.Keys;
        public ICollection<object> Values => _dictionary.Values;

        public DynamicDictionary OrderBy(Func<object, object> keySelector)
        {
            return new DynamicDictionary(_dictionary.OrderBy(pair => keySelector(pair)).ToDictionary(x => x.Key, y => y.Value));
        }

        public DynamicDictionary OrderBy(Func<object, object> keySelector, IComparer<object> comparer)
        {
            return new DynamicDictionary(_dictionary.OrderBy(pair => keySelector(pair), comparer).ToDictionary(x => x.Key, y => y.Value));
        }

        public DynamicDictionary OrderByDescending(Func<dynamic, dynamic> keySelector)
        {
            return new DynamicDictionary(_dictionary.OrderByDescending(pair => keySelector(pair)).ToDictionary(x => x.Key, y => y.Value));
        }

        public DynamicDictionary OrderByDescending(Func<dynamic, dynamic> keySelector, IComparer<dynamic> comparer)
        {
            return new DynamicDictionary(_dictionary.OrderByDescending(pair => keySelector(pair), comparer).ToDictionary(x => x.Key, y => y.Value));
        }

        public IEnumerable<IGrouping<object, KeyValuePair<object, object>>> GroupBy(Func<KeyValuePair<object, object>, object> keySelector)
        {
            return _dictionary.GroupBy(keySelector);
        }

        public IEnumerable<IGrouping<object, KeyValuePair<object, object>>> GroupBy(Func<KeyValuePair<object, object>, object> keySelector,
            IEqualityComparer<object> comparer)
        {
            return _dictionary.GroupBy(keySelector, comparer);
        }

        public IEnumerable<IGrouping<dynamic, dynamic>> GroupBy(Func<KeyValuePair<object, object>, object> keySelector,
            Func<KeyValuePair<object, object>, object> elementSelector)
        {
            return _dictionary.GroupBy(keySelector, elementSelector);
        }

        public IEnumerable<IGrouping<object, object>> GroupBy(Func<KeyValuePair<object, object>, object> keySelector,
            Func<KeyValuePair<object, object>, object> elementSelector, IEqualityComparer<object> comparer)
        {
            return _dictionary.GroupBy(keySelector, elementSelector, comparer);
        }

        public IEnumerable<IGrouping<object, object>> GroupBy(Func<KeyValuePair<object, object>, object> keySelector,
            Func<object, IEnumerable<KeyValuePair<object, object>>, IGrouping<object, object>> resultSelector)
        {
            return _dictionary.GroupBy(keySelector, resultSelector);
        }

        public IEnumerable<IGrouping<object, object>> GroupBy(Func<KeyValuePair<object, object>, object> keySelector,
            Func<KeyValuePair<object, object>, object> elementSelector, Func<object, IEnumerable<object>, IGrouping<object, object>> resultSelector)
        {
            return _dictionary.GroupBy(keySelector, elementSelector, resultSelector);
        }

        public IEnumerable<IGrouping<object, object>> GroupBy(Func<KeyValuePair<object, object>, object> keySelector,
            Func<object, IEnumerable<KeyValuePair<object, object>>, IGrouping<object, object>> resultSelector, IEqualityComparer<object> comparer)
        {
            return _dictionary.GroupBy(keySelector, resultSelector, comparer);
        }

        public KeyValuePair<object, object> Last()
        {
            return _dictionary.Last();
        }

        public KeyValuePair<object, object> Last(Func<KeyValuePair<object, object>, bool> predicate)
        {
            return _dictionary.Last(predicate);
        }

        public KeyValuePair<object, object> LastOrDefault()
        {
            return _dictionary.LastOrDefault();
        }

        public KeyValuePair<object, object> LastOrDefault(Func<KeyValuePair<object, object>, bool> predicate)
        {
            return _dictionary.LastOrDefault(predicate);
        }

        public DynamicDictionary SkipLast(int count)
        {
            return new DynamicDictionary(_dictionary.SkipLast(count));
        }

        public DynamicDictionary Take(int count)
        {
            return new DynamicDictionary(_dictionary.Take(count));
        }

        public DynamicDictionary TakeWhile(Func<KeyValuePair<object, object>, bool> predicate)
        {
            return new DynamicDictionary(_dictionary.TakeWhile(predicate));
        }

        public DynamicDictionary TakeWhile(Func<KeyValuePair<object, object>, int, bool> predicate)
        {
            return new DynamicDictionary(_dictionary.TakeWhile(predicate));
        }

        public DynamicDictionary TakeLast(int count)
        {
            return new DynamicDictionary(_dictionary.Take(count).ToDictionary(x => x.Key, x => x.Value));
        }

        public DynamicDictionary Union(DynamicDictionary second)
        {
            return new DynamicDictionary(_dictionary.Union(second));
        }

        public DynamicDictionary Union(DynamicDictionary second, IEqualityComparer<KeyValuePair<object, object>> comparer)
        {
            return new DynamicDictionary(_dictionary.Union(second, comparer));
        }

        public DynamicDictionary Intersect(DynamicDictionary second)
        {
            return new DynamicDictionary(_dictionary.Intersect(second));
        }

        public DynamicDictionary Intersect(DynamicDictionary second, IEqualityComparer<KeyValuePair<object, object>> comparer)
        {
            return new DynamicDictionary(_dictionary.Intersect(second, comparer));
        }

        public IEnumerable<TResult> OfType<TResult>()
        {
            throw new NotSupportedException();
        }

        public DynamicDictionary Prepend(KeyValuePair<object, object> element)
        {
            return new DynamicDictionary(_dictionary.Prepend(element));
        }

        public DynamicDictionary Where(Func<KeyValuePair<object, object>, bool> predicate)
        {
            return new DynamicDictionary(_dictionary.Where(predicate));
        }

        public DynamicDictionary Where(Func<KeyValuePair<object, object>, int, bool> predicate)
        {
            return new DynamicDictionary(_dictionary.Where(predicate));
        }

        public KeyValuePair<object, object> ElementAt(int index)
        {
            return _dictionary.ElementAt(index);
        }

        public KeyValuePair<object, object> ElementAtOrDefault(int index)
        {
            return _dictionary.ElementAtOrDefault(index);
        }

        public DynamicDictionary Except(IEnumerable<KeyValuePair<object, object>> second)
        {
            return new DynamicDictionary(_dictionary.Except(second));
        }

        public DynamicDictionary Except(IEnumerable<KeyValuePair<object, object>> second, IEqualityComparer<KeyValuePair<object, object>> comparer)
        {
            return new DynamicDictionary(_dictionary.Except(second, comparer));
        }

        public DynamicDictionary Reverse()
        {
            return new DynamicDictionary(_dictionary.Reverse());
        }

        public bool All(Func<KeyValuePair<object, object>, bool> predicate)
        {
            return _dictionary.All(predicate);
        }

        public bool Any()
        {
            throw new NotSupportedException();
        }

        public bool Any(Func<KeyValuePair<object, object>, bool> predicate)
        {
            return _dictionary.Any(predicate);
        }

        public DynamicDictionary Append(KeyValuePair<object, object> element)
        {
            return new DynamicDictionary(_dictionary.Append(element));
        }

        public double Average(Func<dynamic, int> selector)
        {
            return Enumerable.Average(this, selector);
        }

        public double? Average(Func<dynamic, int?> selector)
        {
            return Enumerable.Average(this, selector) ?? DynamicNullObject.Null;
        }

        public double Average(Func<dynamic, long> selector)
        {
            return Enumerable.Average(this, selector);
        }

        public double? Average(Func<dynamic, long?> selector)
        {
            return Enumerable.Average(this, selector) ?? DynamicNullObject.Null;
        }

        public float Average(Func<dynamic, float> selector)
        {
            return Enumerable.Average(this, selector);
        }

        public float? Average(Func<dynamic, float?> selector)
        {
            return Enumerable.Average(this, selector) ?? DynamicNullObject.Null;
        }

        public double Average(Func<dynamic, double> selector)
        {
            return Enumerable.Average(this, selector);
        }

        public double? Average(Func<dynamic, double?> selector)
        {
            return Enumerable.Average(this, selector) ?? DynamicNullObject.Null;
        }

        public decimal Average(Func<dynamic, decimal> selector)
        {
            return Enumerable.Average(this, selector);
        }

        public decimal? Average(Func<dynamic, decimal?> selector)
        {
            return Enumerable.Average(this, selector) ?? DynamicNullObject.Null;
        }

        public KeyValuePair<object, object> Single()
        {
            return _dictionary.Single();
        }

        public KeyValuePair<object, object> Single(Func<KeyValuePair<object, object>, bool> predicate)
        {
            return _dictionary.Single(predicate);
        }

        public KeyValuePair<object, object> SingleOrDefault()
        {
            return _dictionary.SingleOrDefault();
        }

        public KeyValuePair<object, object> SingleOrDefault(Func<KeyValuePair<object, object>, bool> predicate)
        {
            return _dictionary.SingleOrDefault(predicate);
        }

        public DynamicDictionary Skip(int count)
        {
            return new DynamicDictionary(_dictionary.Skip(count));
        }

        public DynamicDictionary SkipWhile(Func<KeyValuePair<object, object>, bool> predicate)
        {
            return new DynamicDictionary(_dictionary.SkipWhile(predicate));
        }

        public DynamicDictionary SkipWhile(Func<KeyValuePair<object, object>, int, bool> predicate)
        {
            return new DynamicDictionary(_dictionary.SkipWhile(predicate));
        }

        public DynamicDictionary DefaultIfEmpty()
        {
            return new DynamicDictionary(_dictionary.DefaultIfEmpty());
        }

        public DynamicDictionary Distinct()
        {
            return new DynamicDictionary(_dictionary.Distinct());
        }

        public DynamicDictionary Distinct(IEqualityComparer<KeyValuePair<object, object>> comparer)
        {
            return new DynamicDictionary(_dictionary.Distinct(comparer));
        }

        public KeyValuePair<object, object> First()
        {
            return _dictionary.First();
        }

        public KeyValuePair<object, object> First(Func<KeyValuePair<object, object>, bool> predicate)
        {
            return _dictionary.First(predicate);
        }

        public KeyValuePair<object, object> FirstOrDefault()
        {
            return _dictionary.FirstOrDefault();
        }

        public KeyValuePair<object, object> FirstOrDefault(Func<KeyValuePair<object, object>, bool> predicate)
        {
            return _dictionary.FirstOrDefault(predicate);
        }

        public DynamicDictionary GroupJoin(DynamicDictionary inner, Func<KeyValuePair<object, object>, object> outerKeySelector, Func<KeyValuePair<object, object>, object> innerKeySelector, Func<KeyValuePair<object, object>, object, KeyValuePair<object, object>> resultSelector)
        {
            return new DynamicDictionary(_dictionary.GroupJoin(inner, outerKeySelector, innerKeySelector, resultSelector));
        }

        public DynamicDictionary GroupJoin(DynamicDictionary inner, Func<KeyValuePair<object, object>, object> outerKeySelector,
            Func<KeyValuePair<object, object>, object> innerKeySelector, Func<KeyValuePair<object, object>, object, KeyValuePair<object, object>> resultSelector, IEqualityComparer<object> comparer)
        {
            return new DynamicDictionary(_dictionary.GroupJoin(inner, outerKeySelector, innerKeySelector, resultSelector, comparer));
        }

        public IEnumerable<dynamic> Join(IDictionary<dynamic, dynamic> items,
            Func<dynamic, dynamic> outerKeySelector,
            Func<dynamic, dynamic> innerKeySelector,
            Func<dynamic, dynamic, dynamic> resultSelector)
        {
            return new DynamicArray(_dictionary.Join(items, x => outerKeySelector(x), y => innerKeySelector(y), (p, k) => resultSelector(p, k)));
        }

        public IEnumerable<dynamic> Join(IDictionary<dynamic, dynamic> items,
            Func<dynamic, dynamic> outerKeySelector,
            Func<dynamic, dynamic> innerKeySelector,
            Func<dynamic, dynamic, dynamic> resultSelector,
            IEqualityComparer<dynamic> comparer)
        {
            return new DynamicArray(_dictionary.Join(items, x => outerKeySelector(x), y => innerKeySelector(y), (p, k) => resultSelector(p, k), comparer));
        }

        public long LongCount()
        {
            return _dictionary.LongCount();
        }

        public long LongCount(Func<KeyValuePair<object, object>, bool> predicate)
        {
            return _dictionary.LongCount(predicate);
        }

        public DynamicDictionary Zip(DynamicDictionary second, Func<KeyValuePair<object, object>, KeyValuePair<object, object>, KeyValuePair<object, object>> resultSelector)
        {
            return new DynamicDictionary(_dictionary.Zip(second, resultSelector));
        }

        public DynamicDictionary Concat(DynamicDictionary second)
        {
            return new DynamicDictionary(_dictionary.Concat(second));
        }

        public dynamic Min()
        {
            throw new NotSupportedException();
        }

        public dynamic Max()
        {
            throw new NotSupportedException();
        }

        public DynamicDictionary SelectMany(Func<KeyValuePair<object, object>, IEnumerable<KeyValuePair<object, object>>> selector)
        {
            return new DynamicDictionary(_dictionary.SelectMany(selector));
        }

        public IEnumerable<KeyValuePair<object, object>> SelectMany(Func<KeyValuePair<object, object>, int, IEnumerable<KeyValuePair<object, object>>> selector)
        {
            return new DynamicDictionary(_dictionary.SelectMany(selector));
        }

        public IEnumerable<KeyValuePair<object, object>> SelectMany(Func<KeyValuePair<object, object>, int, IEnumerable<KeyValuePair<object, object>>> collectionSelector, Func<KeyValuePair<object, object>, KeyValuePair<object, object>, KeyValuePair<object, object>> resultSelector)
        {
            return new DynamicDictionary(_dictionary.SelectMany(collectionSelector, resultSelector));
        }

        public IEnumerable<KeyValuePair<object, object>> SelectMany(Func<KeyValuePair<object, object>, IEnumerable<KeyValuePair<object, object>>> collectionSelector, Func<KeyValuePair<object, object>, KeyValuePair<object, object>, KeyValuePair<object, object>> resultSelector)
        {
            return new DynamicDictionary(_dictionary.SelectMany(collectionSelector, resultSelector));
        }

        public IEnumerable<KeyValuePair<object, object>> Select()
        {
            return _dictionary.Select(x => x);
        }

        public IEnumerable<object> Select(Func<dynamic, dynamic> func)
        {
            return _dictionary.Select(pair => func(pair));
        }

        public IEnumerable<object> Select(Func<dynamic, int, dynamic> func)
        {
            return _dictionary.Select((pair, i) => func(pair, i));
        }

        public int Sum(Func<dynamic, int> selector)
        {
            return Enumerable.Sum(this, selector);
        }

        public int? Sum(Func<dynamic, int?> selector)
        {
            return Enumerable.Sum(this, selector) ?? DynamicNullObject.Null;
        }

        public long Sum(Func<dynamic, long> selector)
        {
            return Enumerable.Sum(this, selector);
        }

        public long? Sum(Func<dynamic, long?> selector)
        {
            return Enumerable.Sum(this, selector) ?? DynamicNullObject.Null;
        }

        public float Sum(Func<dynamic, float> selector)
        {
            return Enumerable.Sum(this, selector);
        }

        public float? Sum(Func<dynamic, float?> selector)
        {
            return Enumerable.Sum(this, selector) ?? DynamicNullObject.Null;
        }

        public double Sum(Func<dynamic, double> selector)
        {
            return Enumerable.Sum(this, selector);
        }

        public double? Sum(Func<dynamic, double?> selector)
        {
            return Enumerable.Sum(this, selector) ?? DynamicNullObject.Null;
        }

        public decimal Sum(Func<dynamic, decimal> selector)
        {
            return Enumerable.Sum(this, selector);
        }

        public decimal? Sum(Func<dynamic, decimal?> selector)
        {
            return Enumerable.Sum(this, selector) ?? DynamicNullObject.Null;
        }
    }
}
