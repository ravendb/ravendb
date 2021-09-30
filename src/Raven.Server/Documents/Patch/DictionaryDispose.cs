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

        new public void Add(ref InternalHandle jsKey, TValue value)
        {
            var jsKeyNew = new InternalHandle(ref jsKey, true);
            try
            {
                base.Add(jsKeyNew, value);
            }
            catch 
            {
                jsKeyNew.Dispose();
                throw;
            }
        }

        new public bool TryAdd(ref InternalHandle jsKey, TValue value)
        {
            var jsKeyNew = new InternalHandle(ref jsKey, true);
            var res = base.TryAdd(jsKeyNew, value);
            if (!res)
            {
                jsKeyNew.Dispose();
            }
            return res;
        }

    }

    public class DictionaryDisposeValueIHV8<TKey> : DictionaryDisposeValue<TKey, InternalHandle>
    {
        public DictionaryDisposeValueIHV8() : base()
        {
        }

        new public void Add(TKey key, ref InternalHandle jsValue)
        {
            var jsValueNew = new InternalHandle(ref jsValue, true);
            try
            {
                base.Add(key, jsValueNew);
            }
            catch 
            {
                jsValueNew.Dispose();
                throw;
            }
        }

        new public bool TryAdd(TKey key, ref InternalHandle jsValue)
        {
            var jsValueNew = new InternalHandle(ref jsValue, true);
            var res = base.TryAdd(key, jsValueNew);
            if (!res)
            {
                jsValueNew.Dispose();
            }
            return res;
        }

        new public bool TryGetValue(TKey key, out InternalHandle jsValue)
        {
            var res = base.TryGetValue(key, out jsValue);
            if (res) {
                jsValue = new InternalHandle(ref jsValue, true);
            }
            return res;
        }

    }

    public class DictionaryDisposeKey<TKey, TValue> : Dictionary<TKey, TValue>, IDisposable
    where TKey : IDisposable
    {
        private bool _disposed = false;

        public DictionaryDisposeKey() : base()
        {
            //GC.SuppressFinalize(this);
        }

        ~DictionaryDisposeKey()
        {
            Dispose(false);
        }

        public void Dispose()
        {  
            Dispose(true);
        }

        protected void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            Clear();

            if (disposing) {
                //GC.SuppressFinalize(this);
            }

            _disposed = true;
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

    public class DictionaryDisposeValue<TKey, TValue> : Dictionary<TKey, TValue>, IDisposable
    where TValue : IDisposable
    {
        private bool _disposed = false;

        public DictionaryDisposeValue() : base()
        {
            //GC.SuppressFinalize(this);
        }

        ~DictionaryDisposeValue()
        {
            Dispose(false);
        }

        public void Dispose()
        {  
            Dispose(true);
        }

        protected void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            Clear();

            if (disposing) {
                //GC.SuppressFinalize(this);
            }

            _disposed = true;
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
