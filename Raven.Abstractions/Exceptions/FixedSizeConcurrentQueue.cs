using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.Exceptions
{
    public class FixedSizeConcurrentQueue<T> : IEnumerable<T>
    {
        private readonly ConcurrentQueue<T> inner = new ConcurrentQueue<T>();
        public int Size { get; private set; }

        public FixedSizeConcurrentQueue(int size)
        {
            Size = size;
        }

        public void Enqueue(T obj)
        {
            inner.Enqueue(obj);
            while (inner.Count > Size)
            {
                T outObj;
                inner.TryDequeue(out outObj);
            }
        }

        public IEnumerator<T> GetEnumerator()
        {
            return inner.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}