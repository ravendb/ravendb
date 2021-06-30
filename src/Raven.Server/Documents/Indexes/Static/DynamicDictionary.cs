using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static
{
    public class DynamicDictionary : DynamicObject, IDictionary<object, object>, IEnumerable<object>
    {
        private readonly Dictionary<object, object> _dictionary;

        public DynamicDictionary(Dictionary<object, object> dictionary)
        {
            _dictionary = dictionary;
        }

        public DynamicDictionary(IEnumerable<KeyValuePair<object, object>> dictionary)
        {
            var tempDictionary = new Dictionary<object, object>();

            foreach (var kvp in dictionary)
            {
                tempDictionary[kvp.Key] = kvp.Value;
            }

            _dictionary = tempDictionary;
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
            return Contains<object, object>(item);
        }

        public bool Contains<TKey, TValue>(KeyValuePair<TKey, TValue> item)
        {
            var key = TypeConverter.KeyAsString(TypeConverter.ToBlittableSupportedType(item.Key));
            var vType = ToDynamicDictionarySupportedType(item.Value);
            var newItem = new KeyValuePair<object, object>(key, vType);

            return _dictionary.Contains(newItem);
        }

        public bool Contains<TKey, TValue>(KeyValuePair<TKey, TValue> item, IEqualityComparer<KeyValuePair<TKey, TValue>> comparer)
        {
            var key = TypeConverter.KeyAsString(TypeConverter.ToBlittableSupportedType(item.Key));
            var vType = ToDynamicDictionarySupportedType(item.Value);
            var newItem = new KeyValuePair<object, object>(key, vType);

            return _dictionary.Contains(newItem, (IEqualityComparer<KeyValuePair<object, object>>)(comparer));
        }

        public DynamicDictionary ToDictionary<T>(Func<T, dynamic> keySelector, Func<T, dynamic> elementSelector)
        {
            return new DynamicDictionary(_dictionary);
        }

        public void CopyTo(KeyValuePair<object, object>[] array, int arrayIndex)
        {
            throw new NotSupportedException();
        }

        public bool Remove(KeyValuePair<object, object> item)
        {
            return Remove<object, object>(item);
        }

        public bool Remove<TKey, TValue>(KeyValuePair<TKey, TValue> item)
        {
            return _dictionary.Remove(item);
        }

        public int Count()
        {
            return _dictionary.Count();
        }

        int ICollection<KeyValuePair<object, object>>.Count => _dictionary.Count();
        public bool IsReadOnly => throw new NotSupportedException();

        public void Add(object key, object value)
        {
            throw new NotSupportedException();
        }

        public bool ContainsKey(object key)
        {
            var newKey = TypeConverter.KeyAsString(TypeConverter.ToBlittableSupportedType(key));
            return _dictionary.ContainsKey(newKey);
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

        public IEnumerable<IGrouping<object, object>> GroupBy(Func<object, object> keySelector)
        {
            return Enumerable.GroupBy(this, keySelector);
        }

        public IEnumerable<IGrouping<object, object>> GroupBy(Func<object, object> keySelector, IEqualityComparer<object> comparer)
        {
            return Enumerable.GroupBy(this, keySelector, comparer);
        }

        public IEnumerable<IGrouping<object, object>> GroupBy(Func<object, object> keySelector, Func<object, object> elementSelector)
        {
            return Enumerable.GroupBy(this, keySelector, elementSelector);
        }

        public IEnumerable<IGrouping<object, object>> GroupBy(Func<object, object> keySelector, Func<object, object> elementSelector, IEqualityComparer<object> comparer)
        {
            return Enumerable.GroupBy(this, keySelector, elementSelector, comparer);
        }

        public IEnumerable<object> GroupBy(Func<object, object> keySelector, Func<object, IEnumerable<object>, object> resultSelector)
        {
            return Enumerable.GroupBy(this, keySelector, resultSelector);
        }

        public IEnumerable<object> GroupBy(Func<object, object> keySelector, Func<object, object> elementSelector, Func<object, IEnumerable<object>, object> resultSelector)
        {
            return Enumerable.GroupBy(this, keySelector, elementSelector, resultSelector);
        }

        public IEnumerable<object> GroupBy(Func<object, object> keySelector, Func<object, IEnumerable<object>, object> resultSelector, IEqualityComparer<object> comparer)
        {
            return Enumerable.GroupBy(this, keySelector, resultSelector, comparer);
        }

        public IEnumerable<object> GroupBy(Func<object, object> keySelector, Func<object, object> elementSelector, Func<object, IEnumerable<object>, object> resultSelector, IEqualityComparer<object> comparer)
        {
            return Enumerable.GroupBy(this, keySelector, elementSelector, resultSelector, comparer);
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
            return new DynamicDictionary(_dictionary.TakeLast(count));
        }

        public DynamicDictionary Union(IEnumerable<object> second)
        {
            var dict = EnumerableToDictionary(second);
            return new DynamicDictionary(_dictionary.Union(dict));
        }

        public DynamicDictionary Union(IEnumerable<object> second, IEqualityComparer<KeyValuePair<object, object>> comparer)
        {
            var dict = EnumerableToDictionary(second);
            return new DynamicDictionary(_dictionary.Union(dict, comparer));
        }

        public DynamicDictionary Intersect(IEnumerable<object> second)
        {
            var dict = EnumerableToDictionary(second);
            return new DynamicDictionary(_dictionary.Intersect(dict));
        }

        public DynamicDictionary Intersect(IEnumerable<object> second, IEqualityComparer<KeyValuePair<object, object>> comparer)
        {
            var dict = EnumerableToDictionary(second);
            return new DynamicDictionary(_dictionary.Intersect(dict, comparer));
        }

        public IEnumerable<TResult> OfType<TResult>()
        {
            return Enumerable.OfType<TResult>(this);
        }

        public DynamicDictionary Prepend<TKey, TValue>(KeyValuePair<TKey, TValue> element)
        {
            var key = TypeConverter.KeyAsString(TypeConverter.ToBlittableSupportedType(element.Key));
            var vType = ToDynamicDictionarySupportedType(element.Value);
            var newItem = new KeyValuePair<object, object>(key, vType);

            return new DynamicDictionary(_dictionary.Prepend(newItem));
        }

        public DynamicDictionary Where(Func<object, bool> predicate)
        {
            var vars = Enumerable.Where(this, predicate);
            var dict = EnumerableToDictionary(vars);

            return new DynamicDictionary(dict);
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

        public DynamicDictionary Except(IEnumerable<object> second)
        {
            var dict = EnumerableToDictionary(second);
            return new DynamicDictionary(_dictionary.Except(dict));
        }

        public DynamicDictionary Except(IEnumerable<object> second, IEqualityComparer<KeyValuePair<object, object>> comparer)
        {
            var dict = EnumerableToDictionary(second);
            return new DynamicDictionary(_dictionary.Except(dict, comparer));
        }

        public DynamicDictionary Reverse()
        {
            return new DynamicDictionary(_dictionary.Reverse());
        }

        public bool All(Func<KeyValuePair<dynamic, dynamic>, bool> predicate)
        {
            return Enumerable.All(this, predicate);
        }

        public bool Any(Func<KeyValuePair<dynamic, dynamic>, bool> predicate)
        {
            return Enumerable.Any(this, predicate);
        }

        public bool Any()
        {
            return _dictionary.Any();
        }

        public DynamicDictionary Append<TKey, TValue>(KeyValuePair<TKey, TValue> element)
        {
            var key = TypeConverter.KeyAsString(TypeConverter.ToBlittableSupportedType(element.Key));
            var vType = ToDynamicDictionarySupportedType(element.Value);
            var newItem = new KeyValuePair<object, object>(key, vType);
            return new DynamicDictionary(_dictionary.Append(newItem));
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
            return Enumerable.LongCount(_dictionary);
        }

        public long LongCount(Func<KeyValuePair<dynamic, dynamic>, bool> predicate)
        {
            return Enumerable.LongCount(this, predicate);
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

        private static dynamic ToDynamicDictionarySupportedType(object value)
        { 
            if (value == null || value is DynamicNullObject)
                return null;

            if (value is DynamicBlittableJson dynamicDocument)
                return dynamicDocument.BlittableJson;

            if (value is string)
                return value;

            if (value is LazyStringValue || value is LazyCompressedStringValue)
                return value;

            if (value is bool)
                return value;

            if (value is int iVal)
                return (long)iVal;

            if (value is short sVal)
                return (long)sVal;

            if (value is long)
                return value;

            if (value is double)
                return value;

            if (value is decimal || value is float || value is byte)
            {
                // TODO: need to create a LazyNumberValue
                throw new NotImplementedException();
            }

            if (value is LazyNumberValue)
                return value;

            if (value is DateTime || value is DateTimeOffset || value is TimeSpan)
                return value;

            if (value is Guid guid)
                return guid.ToString("D");

            return value;
        }

        private static Dictionary<object, object> EnumerableToDictionary(IEnumerable<object> second)
        {
            var dict = new Dictionary<object, object>();
            foreach (var v in second)
            {
                if (v is KeyValuePair<object, object> kvp)
                {
                    var key = TypeConverter.KeyAsString(TypeConverter.ToBlittableSupportedType(kvp.Key));
                    var vType = ToDynamicDictionarySupportedType(kvp.Value);
                    dict[key] = vType;
                }
            }

            return dict;
        }
    }
}
