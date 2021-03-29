using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;

namespace Corax.Tokenizers
{
    public class WhitespaceTokenizer<TSource> : ITokenizer<TSource, WhitespaceTokenizer<TSource>.Enumerator>
        where TSource : ITextSource
    {
        private readonly TokenSpanStorageContext _storage;

        public WhitespaceTokenizer(TokenSpanStorageContext storage)
        {
            _storage = storage;
        }

        public struct Enumerator : IEnumerator<TokenSpan>
        {
            private readonly TSource _source;
            private readonly TokenSpanStorageContext _storage;
            private TokenSpan _current;

            public Enumerator(TSource source, TokenSpanStorageContext storage)
            {
                _source = source;
                _storage = storage;
                _current = TokenSpan.Null;

                Reset();
            }

            public bool MoveNext()
            {
                // We get a chunk of data to work with. The idea is to ensure that
                // we can get a unified view of consecutive memory. 
                int index = 0;
                int startIndex = 0;
                Span<byte> chunk = _source.Peek(128);
                do
                {
                    // We will consume all leading spaces.
                    while (index < chunk.Length && chunk[index] == ' ')
                    {
                        startIndex++;
                        index++;
                    }

                    while (index < chunk.Length)
                    {
                        if (chunk[index] == ' ')
                            goto Found;

                        index++;

                        if (index == chunk.Length)
                            chunk = _source.Peek(chunk.Length * 2);
                    }
                }
                while (index < chunk.Length);

                if (index - startIndex > 0)
                    goto Found;

                // We haven't found any other whitespace
                _source.Consume();
                return false;

            Found:

                // Consume all whitespaces.
                _source.Consume(startIndex);

                // Retrieve a token span that represents the whole word
                // It is important to note that it is the duty of the 
                // source to return a unified TokenSpan of that particular
                // size. Source must handle unification of multiple buffers
                // if they are required.
                TokenSpan result = _source.Retrieve(index - startIndex);
                result.Type = (int)TokenType.Word;

                // Data is ready to be consumed.
                _current = result;
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
