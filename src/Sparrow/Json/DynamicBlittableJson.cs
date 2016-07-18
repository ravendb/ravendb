using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Dynamic;

namespace Sparrow.Json
{
    public class DynamicBlittableJson : DynamicObject
    {
        protected BlittableJsonReaderObject BlittableJsonReaderObject;

        public class DynamicArray : DynamicObject, IEnumerable<object>
        {
            private readonly IEnumerable<object> _inner;

            public DynamicArray(IEnumerable<object> array)
            {
                _inner = array;
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

                if (resultObject is BlittableJsonReaderObject)
                {
                    result = new DynamicBlittableJson((BlittableJsonReaderObject)resultObject);
                }
                else if (resultObject is BlittableJsonReaderArray)
                {
                    result = new DynamicArray((BlittableJsonReaderArray)resultObject);
                }
                else
                {
                    result = resultObject;
                }
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
                return new DynamicArray(_inner.Select(func));
            }

            public IEnumerable<object> Select(Func<IGrouping<object, object>, object> func)
            {
                throw new NotImplementedException();
            }

            public IEnumerable<object> Select(Func<object, int, object> func)
            {
                throw new NotImplementedException();
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

                    var @object = _inner.Current as BlittableJsonReaderObject;
                    if (@object != null)
                    {
                        Current = new DynamicBlittableJson(@object);
                        return true;
                    }

                    var array = _inner.Current as BlittableJsonReaderArray;
                    if (array != null)
                    {
                        Current = new DynamicArray(array);
                        return true;
                    }

                    Current = _inner.Current;
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
        }

        public DynamicBlittableJson(BlittableJsonReaderObject blittableJsonReaderObject)
        {
            BlittableJsonReaderObject = blittableJsonReaderObject;
        }

        public void Set(BlittableJsonReaderObject blittableJsonReaderObject)
        {
            BlittableJsonReaderObject = blittableJsonReaderObject;
        }

        public string[] GetPropertyNames()
        {
            return BlittableJsonReaderObject.GetPropertyNames();
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            return TryGetByName(binder.Name, out result);
        }

        public bool TryGetByName(string name, out object result)
        {
            if (!BlittableJsonReaderObject.TryGetMember(name, out result))
                return false;

            if (result is BlittableJsonReaderObject)
            {
                result = new DynamicBlittableJson((BlittableJsonReaderObject)result);
            }
            else if (result is BlittableJsonReaderArray)
            {
                result = new DynamicArray((BlittableJsonReaderArray)result);
            }

            return true;
        }

        public override bool TryGetIndex(GetIndexBinder binder, object[] indexes, out object result)
        {
            return TryGetByName((string)indexes[0], out result);
        }
    }
}