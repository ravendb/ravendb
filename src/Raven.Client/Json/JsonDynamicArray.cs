using System.Collections;
using System.Collections.Generic;
using System.Dynamic;

namespace Raven.Client.Json
{
    public sealed class JsonDynamicArray : DynamicObject, IList<object>
    {
        private readonly List<object> _items = new();

        public object this[int index]
        {
            get => _items[index];
            set => _items[index] = value;
        }

        public int Count => _items.Count;

        bool ICollection<object>.IsReadOnly => false;

        public void Add(object item) => _items.Add(item);

        public void Clear() => _items.Clear();

        public bool Contains(object item) => _items.Contains(item);

        public void CopyTo(object[] array, int arrayIndex) => _items.CopyTo(array, arrayIndex);

        public int IndexOf(object item) => _items.IndexOf(item);

        public void Insert(int index, object item) => _items.Insert(index, item);

        public bool Remove(object item) => _items.Remove(item);

        public void RemoveAt(int index) => _items.RemoveAt(index);

        public IEnumerator<object> GetEnumerator() => _items.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => _items.GetEnumerator();

        public override string ToString() => JsonDynamicFormatter.ToJson(this);
    }
}
