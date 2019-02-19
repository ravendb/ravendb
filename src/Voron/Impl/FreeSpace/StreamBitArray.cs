//
// Bit Array.cs
//
// Authors:
// Ben Maurer (bmaurer@users.sourceforge.net)
// Marek Safar (marek.safar@gmail.com)
//
// (C) 2003 Ben Maurer
//

//
// Copyright (C) 2004 Novell, Inc (http://www.novell.com)
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
// 
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//

using Sparrow;
using System;
using System.Diagnostics;
using System.IO;
using Sparrow.Server;

namespace Voron.Impl.FreeSpace
{
    public sealed class StreamBitArray
    {
        readonly int[] _inner = new int[64];

        public int SetCount { get; private set; }

        public StreamBitArray()
        {
            
        }

        public StreamBitArray(ValueReader reader)
        {
            if (!BitConverter.IsLittleEndian)
                throw new NotSupportedException("Big endian conversion is not supported yet.");

            SetCount = reader.ReadLittleEndianInt32();

            unsafe
            {
                fixed (int* i = _inner)
                {
                    int read = reader.Read((byte*)i, _inner.Length * sizeof(int));
                    if (read < _inner.Length * sizeof(int))
                        throw new EndOfStreamException();
                }
            }
        }

        public int FirstSetBit()
        {
            for (int i = 0; i < _inner.Length; i++)
            {
                if (_inner[i] == 0)
                    continue;
                return i << 5 | HighestBitSet(_inner[i]);
            }
            return -1;
        }
        

        // Code taken from http://graphics.stanford.edu/~seander/bithacks.html#IntegerLogDeBruijn
        private static readonly int[] MultiplyDeBruijnBitPosition = 
            {
                0, 9, 1, 10, 13, 21, 2, 29, 11, 14, 16, 18, 22, 25, 3, 30,
                8, 12, 20, 28, 15, 17, 24, 7, 19, 27, 23, 6, 26, 5, 4, 31
            };

        private static int HighestBitSet(int v)
        {

            v |= v >> 1; // first round down to one less than a power of 2 
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;

            return MultiplyDeBruijnBitPosition[(uint)(v * 0x07C4ACDDU) >> 27];
        }

        public bool this[int index]
        {
            get { return Get(index); }
            set { Set(index, value); }
        }

        public bool Get(int index)
        {
            return (_inner[index >> 5] & (1 << (index & 31))) != 0;
        }

        public void Set(int index, bool value)
        {
            if (value)
            {
                _inner[index >> 5] |= (1 << (index & 31));
                SetCount++;
            }
            else
            {
                _inner[index >> 5] &= ~(1 << (index & 31));
                SetCount--;
            }
        }

        public int GetEndRangeCount()
        {
            int c = 0;
            for (int i = _inner.Length * 32 -1; i >= 0; i--)
            {
                if (Get(i) == false)
                    break;
                c++;
            }
            return c;
        }

        public bool HasStartRangeCount(int max)
        {
            int c = 0;
            var len = _inner.Length*32;
            for (int i = 0; i < len && c < max; i++)
            {
                if (Get(i) == false)
                    break;
                c++;
            }
            return c == max;
        }

        public Stream ToStream()
        {
            var ms = new MemoryStream(260);

            var tmpBuffer = ToBuffer();

            Debug.Assert(BitConverter.ToInt32(tmpBuffer,0) == SetCount); 

            ms.Write(tmpBuffer, 0, tmpBuffer.Length);
            ms.Position = 0;
            return ms;
        }

        private unsafe byte[] ToBuffer()
        {
            var tmpBuffer = new byte[(_inner.Length + 1)*sizeof (int)];
            unsafe
            {
                fixed (int* src = _inner)
                fixed (byte* dest = tmpBuffer)
                {
                    *(int*) dest = SetCount;
                    Memory.Copy(dest + sizeof (int), (byte*) src, tmpBuffer.Length - 1);
                }
            }
            return tmpBuffer;
        }

        public ByteStringContext.InternalScope ToSlice(ByteStringContext context, out Slice str)
        {
            return ToSlice(context, ByteStringType.Immutable, out str);
        }

        public ByteStringContext.InternalScope ToSlice(ByteStringContext context, ByteStringType type, out Slice str)
        {
            var buffer = ToBuffer();
            ByteString byteString;
            var scope = context.From(buffer, 0, buffer.Length, type, out byteString);
            str = new Slice(byteString);
            return scope;
        }
    }
}
