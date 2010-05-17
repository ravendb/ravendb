using System;
using System.Collections.Generic;
using System.Text;

namespace Lucene.Net.Search.Vectorhighlight
{
    public class HashMap<K, V> : Dictionary<K, V>
    {
        V _NullKeyValue = default(V);

        public new void Add(K key,V value)
        {
            if (key == null)
                _NullKeyValue = value;
            else
                base.Add(key,value);
        }

        public new int Count
        {
            get
            {
                return base.Count + (_NullKeyValue!= null ? 1 : 0);
            }
        }

        public new V this[K key]
        {
            get{
                return Get(key);
            }
            set{
                Add(key,value);
            }
        }

        public V Get(K key)
        {
            if (key == null) return _NullKeyValue;

            V v = default(V);
            base.TryGetValue(key, out v);
            return v;
        }

        public void Put(K key, V val) 
        {
            Add(key,val);
        }
    }
}

namespace System.Runtime.CompilerServices
{
    [AttributeUsage(AttributeTargets.Method)]
    public sealed class ExtensionAttribute : Attribute
    {
        public ExtensionAttribute() { }
    }
}
