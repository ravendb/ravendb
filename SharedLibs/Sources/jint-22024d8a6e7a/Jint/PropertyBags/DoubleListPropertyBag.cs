using System;
using System.Collections.Generic;
using System.Text;
using Jint.Native;

namespace Jint
{
    class DoubleListPropertyBag : IPropertyBag
    {
        private IList<string> keys;
        private IList<Descriptor> values;

        public DoubleListPropertyBag()
        {
            keys = new List<string>(5);
            values = new List<Descriptor>(5);
        }

        #region IPropertyBag Members

        public Descriptor Put(string name, Descriptor descriptor)
        {
            lock (keys)
            {
                keys.Add(name);
                values.Add(descriptor);
            }
            return descriptor;
        }

        public void Delete(string name)
        {
            int index = keys.IndexOf(name);
            keys.RemoveAt(index);
            values.RemoveAt(index);
        }

        public Descriptor Get(string name)
        {
            int index = keys.IndexOf(name);
            return values[index];
        }

        public bool TryGet(string name, out Jint.Native.Descriptor descriptor)
        {
            int index = keys.IndexOf(name);
            if (index < 0)
            {
                descriptor = null;
                return false;
            }
            descriptor = values[index];
            return true;
        }

        public int Count
        {
            get { return keys.Count; }
        }

        public IEnumerable<Descriptor> Values
        {
            get { return values; }
        }

        #endregion

        #region IEnumerable<KeyValuePair<string,Descriptor>> Members

        public IEnumerator<KeyValuePair<string, Descriptor>> GetEnumerator()
        {
            for (int i = 0; i < keys.Count; i++)
                yield return new KeyValuePair<string, Descriptor>(keys[i], values[i]);
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
