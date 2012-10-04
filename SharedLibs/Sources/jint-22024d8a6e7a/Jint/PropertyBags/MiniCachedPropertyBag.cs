using System;
using System.Collections.Generic;
using System.Text;
using Jint.Native;

namespace Jint.PropertyBags
{
    public class MiniCachedPropertyBag : IPropertyBag
    {
        IPropertyBag bag;
        Descriptor lastAccessed;

        public MiniCachedPropertyBag()
        {
            bag = new DictionaryPropertyBag();
        }

        #region IPropertyBag Members

        public Jint.Native.Descriptor Put(string name, Jint.Native.Descriptor descriptor)
        {
            bag.Put(name, descriptor);
            return lastAccessed = descriptor;
        }

        public void Delete(string name)
        {
            bag.Delete(name);
            if (lastAccessed != null && lastAccessed.Name == name)
                lastAccessed = null;
        }

        public Jint.Native.Descriptor Get(string name)
        {
            if (lastAccessed != null && lastAccessed.Name == name)
                return lastAccessed;
            Descriptor descriptor = bag.Get(name);
            if (descriptor != null)
                lastAccessed = descriptor;
            return descriptor;
        }

        public bool TryGet(string name, out Jint.Native.Descriptor descriptor)
        {
            if (lastAccessed != null && lastAccessed.Name == name)
            {
                descriptor = lastAccessed;
                return true;
            }
            bool result = bag.TryGet(name, out descriptor);
            if (result)
                lastAccessed = descriptor;
            return result;
        }

        public int Count
        {
            get { return bag.Count; }
        }

        public IEnumerable<Jint.Native.Descriptor> Values
        {
            get { return bag.Values; }
        }

        #endregion

        #region IEnumerable<KeyValuePair<string,Descriptor>> Members

        public IEnumerator<KeyValuePair<string, Jint.Native.Descriptor>> GetEnumerator()
        {
            return bag.GetEnumerator();
        }

        #endregion

        #region IEnumerable Members

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion
    }
}
