using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Utils
{
    public class StatefulEnumerator<T> : IEnumerable<T>
    {
        private readonly IEnumerable<T> _items;
        public T Current;

        public StatefulEnumerator(IEnumerable<T> items)
        {
            _items = items;
        }

        public IEnumerator<T> GetEnumerator()
        {
            foreach (var item in _items)
            {
                Current = item;
                yield return item;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}