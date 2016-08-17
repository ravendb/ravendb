using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using Raven.Client.Linq;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Indexes.Static
{
    public class DynamicArray : DynamicObject, IEnumerable<object>
    {
        private readonly IEnumerable<object> _inner;

        public DynamicArray(IEnumerable inner)
            : this(inner.Cast<object>())
        {
        }

        public DynamicArray(IEnumerable<object> inner)
        {
            _inner = inner;
        }

        public int Length => _inner.Count();

        public int Count => _inner.Count();

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            const string LengthName = "Length";
            const string CountName = "Count";
            result = null;
            if (string.CompareOrdinal(binder.Name, LengthName) == 0 ||
                string.CompareOrdinal(binder.Name, CountName) == 0)
            {
                result = Length;
                return true;
            }

            return false;
        }

        public override bool TryGetIndex(GetIndexBinder binder, object[] indexes, out object result)
        {
            var i = (int)indexes[0];
            var resultObject = _inner.ElementAt(i);

            result = TypeConverter.DynamicConvert(resultObject);
            return true;
        }

        public IEnumerator<object> GetEnumerator()
        {
            return new DynamicArrayIterator(_inner);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public IEnumerable<object> Select(Func<object, object> func)
        {
            return new DynamicArray(Enumerable.Select(this, func));
        }

        public IEnumerable<object> Select(Func<IGrouping<object, object>, object> func)
        {
            return new DynamicArray(Enumerable.Select(this, o => func((IGrouping<object, object>)o)));
        }

        public IEnumerable<object> Select(Func<object, int, object> func)
        {
            return new DynamicArray(Enumerable.Select(this, func));
        }

        public IEnumerable<object> SelectMany(Func<object, IEnumerable<object>> func)
        {
            return new DynamicArray(Enumerable.SelectMany(this, func));
        }

        public IEnumerable<object> SelectMany(Func<object, IEnumerable<object>> func, Func<object, object, object> selector)
        {
            return new DynamicArray(Enumerable.SelectMany(this, func, selector));
        }

        public IEnumerable<object> SelectMany(Func<object, int, IEnumerable<object>> func)
        {
            return new DynamicArray(Enumerable.SelectMany(this, func));
        }

        public dynamic GroupBy(Func<dynamic, dynamic> keySelector)
        {
            return new DynamicArray(Enumerable.GroupBy(this, keySelector).Select(x => new DynamicGrouping(x)));
        }

        public dynamic GroupBy(Func<dynamic, dynamic> keySelector, Func<dynamic, dynamic> selector)
        {
            return new DynamicArray(Enumerable.GroupBy(this, keySelector, selector).Select(x => new DynamicGrouping(x)));
        }

        public decimal Sum(Func<object, decimal> selector)
        {
            return Enumerable.Sum(this, selector);
        }

        public dynamic DefaultIfEmpty(object defaultValue = null)
        {
            return Enumerable.DefaultIfEmpty(this, defaultValue ?? DynamicNullObject.Null);
        }

        private class DynamicArrayIterator : IEnumerator<object>
        {
            private readonly IEnumerator<object> _inner;

            public DynamicArrayIterator(IEnumerable<object> items)
            {
                _inner = items.GetEnumerator();
            }

            public bool MoveNext()
            {
                if (_inner.MoveNext() == false)
                    return false;


                Current = TypeConverter.DynamicConvert(_inner.Current);
                return true;
            }

            public void Reset()
            {
                throw new NotImplementedException();
            }

            public object Current { get; private set; }

            object IEnumerator.Current => Current;

            public void Dispose()
            {
            }
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;

            if (ReferenceEquals(this, obj))
                return true;

            var array = obj as DynamicArray;

            if (array != null)
                return Equals(_inner, array._inner);

            return Equals(_inner, obj);
        }

        public override int GetHashCode()
        {
            return _inner?.GetHashCode() ?? 0;
        }

        private class DynamicGrouping : DynamicArray, IGrouping<object, object>
        {
            private readonly IGrouping<dynamic, dynamic> _grouping;

            public DynamicGrouping(IGrouping<dynamic, dynamic> grouping)
                : base(grouping)
            {
                _grouping = grouping;
            }

            public dynamic Key => _grouping.Key;

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }
    }
}