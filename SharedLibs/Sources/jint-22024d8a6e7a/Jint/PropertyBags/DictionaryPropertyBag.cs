using System;
using System.Collections.Generic;
using System.Text;
using Jint.Native;

namespace Jint
{
    class DictionaryPropertyBag : IPropertyBag
    {
        private Dictionary<string, Descriptor> bag = new Dictionary<string, Descriptor>(5);

        #region IPropertyBag Members

        public Descriptor Put(string name, Descriptor descriptor)
        {
            // replace existing without any exception
            bag[name] = descriptor;
            return descriptor;
        }

        public void Delete(string name)
        {
            bag.Remove(name);
        }

        public Jint.Native.Descriptor Get(string name)
        {
            Descriptor desc;
            TryGet(name, out desc);
            return desc;
        }

        public bool TryGet(string name, out Jint.Native.Descriptor descriptor)
        {
           return bag.TryGetValue(name, out descriptor);
        }

        public int Count
        {
            get { return bag.Count; }
        }

        #endregion

        #region IPropertyBag Members


        public IEnumerable<Descriptor> Values
        {
            get { return bag.Values; }
        }

        #endregion

        #region IEnumerable<KeyValuePair<string,Descriptor>> Members

        public IEnumerator<KeyValuePair<string, Descriptor>> GetEnumerator()
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
