using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Server.Binary
{
    public ref struct BitReader
    {
        private int _length;
        private byte _shift;
        private ReadOnlySpan<byte> _data;

        public BitReader(ReadOnlySpan<byte> data)
        {
            _length = data.Length * 8;
            _data = data;
            _shift = 0;
        }

        public BitReader(ReadOnlySpan<byte> data, int length, int skipped = 0)
        {
            _length = length;
            _data = data;
            _shift = 0;

            if (skipped != 0)
                Skip(skipped);
        }

        public int Length => _length;

        public void Skip(int bits)
        {
            int bytesSkipped = (_shift + bits) / 8;

            _data = _data.Slice(bytesSkipped);
            _shift = (byte)((_shift + bits) % 8);
            _length -= bits;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Bit Read()
        {
            if (_length == 0)
                throw new InvalidOperationException("Cannot read from a 0 length stream.");

            int value = _data[0] >> (7 - _shift);
            _length--;
            _shift++;

            if (_shift >= 8) // Will wrap around. 
            {
                _data = _data.Slice(1);
                _shift = 0;
            }

            return new Bit((byte)value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Bit Peek()
        {
            throw new NotImplementedException("Not supported yet.");
        }
    }
}
