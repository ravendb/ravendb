using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Corax.Filters
{
    public sealed class LowerCaseFilter<TSource> : ITokenFilter<TSource, LowerCaseFilter<TSource>.Enumerator>
        where TSource : IEnumerator<TokenSpan>
    {
        private readonly TokenSpanStorageContext _storage;

        public LowerCaseFilter([NotNull] TokenSpanStorageContext storage)
        {
            _storage = storage;
        }

        public struct Enumerator: IEnumerator<TokenSpan>
        {
            private readonly TokenSpanStorageContext _storage;
            private TSource _source;
            private TokenSpan _current;

            public Enumerator([NotNull] TSource source, [NotNull] TokenSpanStorageContext storage)
            {
                _source = source;
                _storage = storage;
                _current = TokenSpan.Null;

                Reset();
            }

            public bool MoveNext()
            {
                bool moveNext = _source.MoveNext();
                if (moveNext)
                {
                    _current = _source.Current;

                    var buffer = _storage.RequestWriteAccess(_current);
                    DoLowercase(buffer);
                }
                return moveNext;
            }

            private void DoLowercase(Span<byte> buffer)
            {
                for (int i = 0; i < buffer.Length; i++)
                {
                    if (buffer[i] >= 'A' && buffer[i] <= 'Z')
                    {
                        byte value = buffer[i];
                        value += (byte)('a' - 'A');
                        buffer[i] = value;
                    }
                }
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

        public Enumerator Filter(TSource source)
        {
            return new(source, _storage);
        }

        public void Dispose()
        {
        }
    }
}
