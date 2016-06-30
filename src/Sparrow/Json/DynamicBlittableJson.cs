using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;

namespace Sparrow.Json
{
    public class DynamicBlittableJson : DynamicObject
    {
        protected BlittableJsonReaderObject BlittableJsonReaderObject;

        public class DynamicBlittableArray : DynamicObject, IEnumerable<object>
        {
            protected BlittableJsonReaderArray BlittableJsonReaderArray;

            public DynamicBlittableArray(BlittableJsonReaderArray blittableJsonReaderArray)
            {
                BlittableJsonReaderArray = blittableJsonReaderArray;
            }

            public int Length => BlittableJsonReaderArray.Length;

            public int Count => BlittableJsonReaderArray.Length;

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
                var i = (int)(indexes[0]);
                var resultObject = BlittableJsonReaderArray[i];

                if (resultObject is BlittableJsonReaderObject)
                {
                    result = new DynamicBlittableJson((BlittableJsonReaderObject)resultObject);
                }
                else if (resultObject is BlittableJsonReaderArray)
                {
                    result = new DynamicBlittableArray((BlittableJsonReaderArray)resultObject);
                }
                else
                {
                    result = resultObject;
                }
                return true;
            }

            public IEnumerator<object> GetEnumerator()
            {
                return new DynamicBlittableArrayIterator(BlittableJsonReaderArray.Items);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            private class DynamicBlittableArrayIterator : IEnumerator<object>
            {
                private readonly IEnumerator<object> _inner;

                public DynamicBlittableArrayIterator(IEnumerable<object> items)
                {
                    _inner = items.GetEnumerator();
                }

                public bool MoveNext()
                {
                    if (_inner.MoveNext() == false)
                        return false;

                    if (_inner.Current is BlittableJsonReaderObject)
                    {
                        Current = new DynamicBlittableJson((BlittableJsonReaderObject)_inner.Current);
                        return true;
                    }

                    if (_inner.Current is BlittableJsonReaderArray)
                    {
                        Current = new DynamicBlittableArray((BlittableJsonReaderArray)_inner.Current);
                        return true;
                    }

                    if (_inner.Current is LazyStringValue)
                    {
                        Current = _inner.Current;
                        return true;
                    }

                    throw new NotSupportedException("Unknown blittable object");
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
            return TryGet(binder.Name, out result);
        }

        private bool TryGet(string name, out object result)
        {
            if (!BlittableJsonReaderObject.TryGetMember(name, out result))
                return false;

            if (result is BlittableJsonReaderObject)
            {
                result = new DynamicBlittableJson((BlittableJsonReaderObject)result);
            }
            else if (result is BlittableJsonReaderArray)
            {
                result = new DynamicBlittableArray((BlittableJsonReaderArray)result);
            }

            return true;
        }

        public override bool TryGetIndex(GetIndexBinder binder, object[] indexes, out object result)
        {
            return TryGet((string)indexes[0], out result);
        }
    }
}