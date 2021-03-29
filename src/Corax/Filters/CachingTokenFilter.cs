using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Sparrow.Collections;

namespace Corax.Filters
{
    public sealed class CachingTokenFilter<TSource> : ITokenFilter<TSource, CachingTokenFilter<TSource>.Enumerator>
            where TSource : IEnumerator<TokenSpan>
    {
        private readonly TokenSpanStorageContext _storage;
        
        protected FastList<TokenSpan> Cache;

        public CachingTokenFilter([NotNull] TokenSpanStorageContext storage)
        {
            _storage = storage;
        }

        public struct Enumerator : IEnumerator<TokenSpan>
        {
            private TSource _source;
            private TokenSpan _current;
            private CachingTokenFilter<TSource> _parent;

            private int _index;

            public Enumerator([NotNull] TSource source, CachingTokenFilter<TSource> parent)
            {
                _source = source;
                _current = TokenSpan.Null;
                _parent = parent;
                _index = -1;

                // We allocate the cache and consume the whole sequence. 
                var cache = TokenSpan.SequencesPool.Allocate();
                foreach (var token in _source)
                    cache.Add(token);

                _parent.Cache = cache;

                Reset();
            }

            public bool MoveNext()
            {
                // Increment the index and go on
                _index++;
                if (_index < _parent.Cache.Count)
                {
                    _current = _parent.Cache[_index];
                    return true;
                }

                _current = TokenSpan.Null;
                return false;
            }

            public void Reset()
            {
                _index = -1;
                _current = TokenSpan.Null;
                _source.Reset();
            }

            public TokenSpan Current => _current;

            object IEnumerator.Current => _current;

            public void Dispose()
            {
            }
        }

        public Enumerator Filter(TSource source)
        {
            return new(source, this);
        }

        public void Dispose()
        {
            if (Cache != null)
            {
                TokenSpan.SequencesPool.Free(Cache);
                Cache = null;
            }
        }
    }
}
