using System;
using System.Collections;
using JetBrains.Annotations;

namespace Raven.Server.Documents.Indexes.Static
{
    public class TimeCountingEnumerable : IEnumerable
    {
        [NotNull]
        private readonly IEnumerable _enumerable;

        [NotNull]
        private readonly IndexingStatsScope _stats;

        public TimeCountingEnumerable([NotNull] IEnumerable enumerable, [NotNull] IndexingStatsScope stats)
        {
            _enumerable = enumerable;
            _stats = stats;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new Enumerator(_enumerable.GetEnumerator(), _stats);
        }

        public Enumerator GetEnumerator()
        {
            return new Enumerator(_enumerable.GetEnumerator(), _stats);
        }

        public readonly struct Enumerator : IEnumerator
        {
            [NotNull]
            private readonly IEnumerator _enumerator;
            [NotNull]
            private readonly IndexingStatsScope _stats;

            public Enumerator([NotNull] IEnumerator enumerator, [NotNull] IndexingStatsScope stats)
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
