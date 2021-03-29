using System;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace Corax
{

    /// <summary>
    /// The StringTextSource is typically used for testing purposes. Unless strictly necessary we should avoid
    /// using it. Use the LazyStringValueSource instead. 
    /// </summary>
    public class StringTextSource : ITextSource
    {
        private readonly TokenSpanStorageContext _context;
        private readonly byte[] _value;
        private int _index;

        public StringTextSource([NotNull] TokenSpanStorageContext context, string value)
        {
            _context = context;
            _value = Encoding.UTF8.GetBytes(value);
            _index = -1;
        }

        public Span<byte> Peek(int size)
        {
            // Return the requested bytes.
            return new (_value, _index, Math.Min(size, _value.Length - _index));
        }

        public void Consume(int size = -1)
        {
            // Nothing to do.
            if (size == 0)
                return;

            _index = size == -1 ? _value.Length : Math.Min(_value.Length, _index + size);
        }

        public TokenSpan Retrieve(int length, int type)
        {
            // We allocate the TokenSpan on the context
            var destination = _context.Allocate(out TokenSpan token,length, type);

            // We copy the source data into the context allocated memory
            var source = new Span<byte>(_value, _index, length);
            source.CopyTo(destination);

            // We consume the bytes
            _index += length;

            // If we are out of bounds we throw an exception (should not happen).
            if (_index > _value.Length)
                throw new ArgumentOutOfRangeException(nameof(length));

            // We return the token.
            return token;
        }

        public void Reset()
        {
            // We reset the source to be reused.
            _index = 0;
        }

        public void Dispose()
        {
        }
    }
}
