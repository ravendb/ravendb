using System;
using System.Collections.Generic;
using V8.Net;

namespace Raven.Server.Documents.Patch
{
    public class DictionaryDisposeKeyIHV8<TValue> : DictionaryDisposeKey<InternalHandle, TValue>
    {
        public DictionaryDisposeKeyIHV8() : base()
        {
        }

        new public void Add(InternalHandle key, TValue value)
        {
            var keyNew = new InternalHandle(key, true);
            try
            {
                base.Add(keyNew, value);
            }
            catch 
            {
                keyNew.Dispose();
                throw;
            }
        }

        new public bool TryAdd(InternalHandle key, TValue value)
        {
            var keyNew = new InternalHandle(key, true);
            var res = base.TryAdd(keyNew, value);
            if (!res)
            {
                keyNew.Dispose();
            }
            return res;
        }

    }

    public class DictionaryDisposeValueIHV8<TKey> : DictionaryDisposeValue<TKey, InternalHandle>
    {
        public DictionaryDisposeValueIHV8() : base()
        {
        }

        new public void Add(TKey key, InternalHandle value)
        {
            var valueNew = new InternalHandle(value, true);
            try
            {
                base.Add(key, valueNew);
            }
            catch 
            {
                valueNew.Dispose();
                throw;
            }
        }

        new public bool TryAdd(TKey key, InternalHandle value)
        {
            var valueNew = new InternalHandle(value, true);
            var res = base.TryAdd(key, valueNew);
            if (!res)
            {
                valueNew.Dispose();
            }
            return res;
        }

        new public bool TryGetValue(TKey key, out InternalHandle jsValue)
        {
            var res = base.TryGetValue(key, out jsValue);
            if (res) {
                jsValue = new InternalHandle(jsValue, true);
            }
            return res;
        }

    }

    public class DictionaryDisposeKey<TKey, TValue> : Dictionary<TKey, TValue>
    where TKey : IDisposable
    {
        public DictionaryDisposeKey() : base()
        {
        }

        ~DictionaryDisposeKey()
        {
            Clear();
        }

        new public void Clear()
        {
            foreach (var kvp in this)
                kvp.Key.Dispose();
            base.Clear();
        }


        new public bool Remove(TKey key)
        {
            var res = base.Remove(key);
            if (res)
                key.Dispose();
            return res;
        }
        new public bool Remove(TKey key, out TValue value)
        {
            var res = base.Remove(key, out value);
            if (res)
                key.Dispose();
            return res;
        }
    }

    public class DictionaryDisposeValue<TKey, TValue> : Dictionary<TKey, TValue>
    where TValue : IDisposable
    {
        public DictionaryDisposeValue() : base()
        {
        }

        ~DictionaryDisposeValue()
        {
            Clear();
        }

        new public void Clear()
        {
            foreach (var kvp in this)
                kvp.Value.Dispose();
            base.Clear();
        }

        new public bool Remove(TKey key)
        {
            var res = base.Remove(key, out TValue value);
            if (res)
                value.Dispose();
            return res;
        }
    }

}
