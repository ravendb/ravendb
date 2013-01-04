using System;
using System.Collections.Generic;
using System.Text;
using Jint.Marshal;

namespace Jint.Native {
    [Serializable]
    public sealed class JsArray : JsObject {
        private int length = 0;

        SortedList<int, JsInstance> m_data = new SortedList<int, JsInstance>();

        public JsArray(JsObject prototype)
            : base(prototype) {
        }

        private JsArray(SortedList<int, JsInstance> data, int len, JsObject prototype)
            : base(prototype) {
            m_data = data;
            length = len;
        }

        public override bool IsClr
        {
            get
            {
                return false;
            }
        }

        /// <summary>
        /// 15.4.2
        /// </summary>
        public override string Class
        {
            get
            {
                return CLASS_ARRAY;
            }
        }

        public override bool ToBoolean() {
            return true;
        }

        public override int Length {
            get {
                return length;
            }
            set {
                setLength(value);
            }
        }

        public override JsInstance this[string index] {
            get {
                int i;
                if (Int32.TryParse(index, out i))
                    return get(i);
                else
                    return base[index];
            }
            set {
                int i;
                if (Int32.TryParse(index,out i))
                    put(i,value);
                else
                    base[index] = value;
            }
        }

        /// <summary>
        /// Overriden indexer to optimize cases when we already have a number
        /// </summary>
        /// <param name="key">index</param>
        /// <returns>value</returns>
        public override JsInstance this[JsInstance key] {
            get {
                double keyNumber = key.ToNumber();
                int i = (int)keyNumber;
                if (i == keyNumber && i >= 0) {
                    // we have got an index
                    return this.get(i);
                }
                else {
                    return base[key.ToString()];
                }
            }
            set {
                double keyNumber = key.ToNumber();
                int i = (int)keyNumber;
                if (i == keyNumber && i >= 0) {
                    // we have got an index
                    this.put(i, value);
                }
                else {
                    base[key.ToString()] = value;
                }
            }
        }

        public override void DefineOwnProperty(Descriptor d) {
            int index;
            if(int.TryParse(d.Name, out index))
                put(index, d.Get(this));
            else
                base.DefineOwnProperty(d);
        }

        public JsInstance get(int i) {
            JsInstance value;
            return m_data.TryGetValue(i, out value) && value != null ? value : JsUndefined.Instance;
        }

        public JsInstance put(int i, JsInstance value) {
            if (i >= length)
                length = i + 1;
            return m_data[i] = value;
        }

        private void setLength(int newLength) {
            if (newLength < 0)
                throw new ArgumentOutOfRangeException("New length is out of range");

            if (newLength < length) {
                int keyIndex = FindKeyOrNext(newLength);
                if (keyIndex >= 0) {
                    for (int i = m_data.Count - 1; i >= keyIndex; i--)
                        m_data.RemoveAt(i);
                }
            }
            length = newLength;
        }

        public override bool TryGetProperty(string key, out JsInstance result) {
            result = JsUndefined.Instance;

            int index;
            if(int.TryParse(key, out index))
                return m_data.TryGetValue(Convert.ToInt32(index), out result);
            else
                return base.TryGetProperty(key, out result);
            
        }

        private int FindKeyOrNext(int key) {
            int left = 0, right = m_data.Count - 1;
            int index = 0;
            while (left <= right) {
                int current = m_data.Keys[index];
                if (current == key)
                    return index;
                else {
                    if (current > key)
                        right = index - 1;
                    else
                        left = index + 1;
                    index = (left + right) / 2;
                }
            }

            // not found, left will contain next after index if it's in range
            return left < m_data.Count ? left : -1;

        }

        private int FindKeyOrPrev(int key) {
            int left = 0, right = m_data.Count - 1;
            int index = 0;
            while (left <= right) {
                int current = m_data.Keys[index];
                if (current == key)
                    return index;
                else {
                    if (current > key)
                        right = index - 1;
                    else
                        left = index + 1;
                    index = (left + right) / 2;
                }
            }

            // not found, right will contain previous before index if it's in range
            return right;
        }

        public override void Delete(JsInstance key) {
            double keyNumber = key.ToNumber();
            int index = (int)keyNumber;
            if (index == keyNumber)
                m_data.Remove(index);
            else
                base.Delete(key.ToString());
        }

        public override void Delete(string key) {
            int index;
            if(int.TryParse(key, out index))
                m_data.Remove(index);
            else
                base.Delete(key);
        }

        #region array specific methods

        [RawJsMethod]
        public JsArray concat(IGlobal global, JsInstance[] args) {
            var newData = new SortedList<int, JsInstance>(m_data);
            int offset = length;
            foreach (var item in args) {
                if (item is JsArray) {
                    foreach (var pair in ((JsArray)item).m_data)
                        newData.Add(pair.Key + offset, pair.Value);
                    offset += ((JsArray)item).Length;
                }
                else if (global.ArrayClass.HasInstance(item as JsObject)) {
                    // Array subclass
                    JsObject obj = (JsObject)item;

                    for (int i = 0; i < obj.Length; i++) {
                        JsInstance value;
                        if (obj.TryGetProperty(i.ToString(), out value))
                            newData.Add(offset + i, value);
                    }
                }
                else {
                    newData.Add(offset, item);
                    offset++;
                }
            }

            return new JsArray(newData, offset, global.ArrayClass.PrototypeProperty);
        }

        [RawJsMethod]
        public JsString join(IGlobal global, JsInstance separator) {
            if (length == 0)
                return global.StringClass.New();

            string sep = separator == JsUndefined.Instance ? "," : separator.ToString();
            string[] map = new string[length];

            JsInstance item;
            for (int i = 0; i < length; i++)
                map[i] = m_data.TryGetValue(i, out item) && item != JsNull.Instance && item != JsUndefined.Instance ? item.ToString() : "";

            return global.StringClass.New(String.Join(sep, map));
        }

        #endregion


        public override string ToString() {
            var list = m_data.Values;
            string[] values = new string[list.Count];
            for (int i = 0; i < list.Count; i++) {
                if (list[i] != null)
                    values[i] = list[i].ToString();
            }

            return String.Join(",", values);
        }

        public override JsInstance ToPrimitive(IGlobal global) {
            if (global == null)
                throw new ArgumentNullException();
            return global.StringClass.New(ToString());
        }

        IEnumerable<string> baseGetKeys()
        {
            return base.GetKeys();
        }

        public override IEnumerable<string> GetKeys() {
            var keys = m_data.Keys;
            for (int i = 0; i < keys.Count; i++)
                yield return keys[i].ToString();

            foreach (var key in baseGetKeys()) 
                yield return key;
        }

        IEnumerable<JsInstance> baseGetValues()
        {
            return base.GetValues();
        }

        public override IEnumerable<JsInstance> GetValues() {
            var vals = m_data.Values;
            for (int i = 0; i < vals.Count; i++)
                yield return vals[i];
            foreach (var val in baseGetValues())
                yield return val;
        }

        public override bool HasOwnProperty(string key) {
            int index;
            if(int.TryParse(key, out index))
                return index >= 0 && index < length ? m_data.ContainsKey(index) : false;
            else
                return base.HasOwnProperty(key);
        }

        public override double ToNumber() {
            return Length;
        }
    }
}
