using System.Collections;
using System.Collections.Generic;

namespace Raven.Database.Indexing
{
    public class StatefulEnumerableWrapper<T> : IEnumerable<T>
    {
        private readonly IEnumerator<T> inner;

        public StatefulEnumerableWrapper(IEnumerator<T> inner)
        {
            this.inner = inner;
        }

        public class StatefulbEnumeratorWrapper : IEnumerator<T>
        {
            private readonly IEnumerator<T> inner;

            public StatefulbEnumeratorWrapper(IEnumerator<T> inner)
            {
                this.inner = inner;
            }

            public void Dispose()
            {
                inner.Dispose();
            }

            public bool MoveNext()
            {
                return inner.MoveNext();
            }

            public void Reset()
            {
                inner.Reset();
            }

            public T Current
            {
                get { return inner.Current; }
            }

            object IEnumerator.Current
            {
                get { return Current; }
            }
        }

        public T Current
        {
            get { return inner.Current; }
        }

        public IEnumerator<T> GetEnumerator()
        {
            return new StatefulbEnumeratorWrapper(inner);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}