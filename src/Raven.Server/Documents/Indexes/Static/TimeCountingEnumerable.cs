using System;
using System.Collections;

namespace Raven.Server.Documents.Indexes.Static
{
    public class TimeCountingEnumerable : IEnumerable
    {
        private readonly IEnumerable _enumerable;
        private readonly IndexingStatsScope _stats;

        public TimeCountingEnumerable(IEnumerable enumerable, IndexingStatsScope stats, string name)
        {
            _enumerable = enumerable;
            _stats = stats.For(name, start: false);
        }

        public IEnumerator GetEnumerator()
        {
            return new Enumerator(_enumerable.GetEnumerator(), _stats);
        }

        private class Enumerator : IEnumerator
        {
            private readonly IEnumerator _enumerator;
            private readonly IndexingStatsScope _stats;

            public Enumerator(IEnumerator enumerator, IndexingStatsScope stats)
            {
                _enumerator = enumerator;
                _stats = stats;
            }

            public bool MoveNext()
            {
                using (_stats.Start())
                    return _enumerator.MoveNext();
            }

            public void Reset()
            {
                throw new NotSupportedException();
            }

            public object Current => _enumerator.Current;
        }
    }
}