using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace Corax.Filters
{
    public abstract class FilteringTokenFilter<TSource> : ITokenFilter<TSource, FilteringTokenFilter<TSource>.Enumerator>
        where TSource : IEnumerator<TokenSpan>
    {
        protected readonly TokenSpanStorageContext Storage;

        protected FilteringTokenFilter([NotNull] TokenSpanStorageContext storage)
        {
            Storage = storage;
        }

        public struct Enumerator : IEnumerator<TokenSpan>
        {
            private readonly FilteringTokenFilter<TSource> _parent;
            private TSource _source;
            private TokenSpan _current;
            
            public Enumerator([NotNull] TSource source, FilteringTokenFilter<TSource> parent)
            {
                _source = source;
                _current = TokenSpan.Null;
                _parent = parent;

                Reset();
            }

            public bool MoveNext()
            {
                bool moveNext = _source.MoveNext();
                if (moveNext)
                {
                    _current = _source.Current;
                    while (moveNext && !_parent.AcceptToken(in _current))
                    {
                        moveNext = _source.MoveNext();
                        _current = _source.Current;
                    }
                }

                if (!moveNext)
                    _current = TokenSpan.Null;

                return moveNext;
            }

            public void Reset()
            {
                _current = TokenSpan.Null;
                _source.Reset();
            }

            public TokenSpan Current => _current;

            object IEnumerator.Current => _current;

            public void Dispose() { }
        }

        protected abstract bool AcceptToken(in TokenSpan token);

        public Enumerator Filter(TSource source)
        {
            return new(source, this);
        }

        protected virtual void Dispose(bool disposing)
        {
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
