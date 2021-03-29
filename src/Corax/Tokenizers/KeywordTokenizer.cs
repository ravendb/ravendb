using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Corax.Tokenizers
{
    public class KeywordTokenizer<TSource> : ITokenizer<TSource, KeywordTokenizer<TSource>.Enumerator>
        where TSource : ITextSource
    {
        private readonly TokenSpanStorageContext _storage;

        public KeywordTokenizer(TokenSpanStorageContext storage)
        {
            _storage = storage;
        }

        public struct Enumerator : IEnumerator<TokenSpan>
        {
            private readonly TokenSpanStorageContext _storage;
            private TokenSpan _current;
            private TSource _source;

            public Enumerator(TSource source, TokenSpanStorageContext storage)
            {
                _source = source;
                _storage = storage;
                _current = TokenSpan.Null;

                Reset();
            }

            public bool MoveNext()
            {
                int chunkSize = 128;
                // We get a chunk of data to work with. The idea is to ensure that
                // we can get a unified view of consecutive memory. 
                Span<byte> chunk = _source.Peek(chunkSize);
                if (chunk.Length == 0)
                    return false;

                // Resize the chunk if it is bigger than what we got.
                while (chunkSize == chunk.Length)
                {
                    chunkSize = chunkSize * 2;
                    chunk = _source.Peek(chunkSize);
                }

                // Retrieve a token span that represents the whole word
                _current = _source.Retrieve(chunk.Length, type: TokenType.Keyword);
                return true;
            }

            public void Reset()
            {
                _source.Reset();
            }

            public TokenSpan Current => _current;

            object IEnumerator.Current => _current;

            public void Dispose() { }
        }

        public Enumerator Tokenize(TSource source)
        {
            return new(source, _storage);
        }

        public void Dispose()
        {
        }
    }
}
