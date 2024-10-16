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
using System.Collections;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace Voron.Impl.FreeSpace
{
    public sealed class StreamBitArray
    {
        private const int CountOfWords = 64;
        private const int BitsInWord = 32;
        private const int TotalBits = CountOfWords * BitsInWord;

        readonly int[] _inner = new int[CountOfWords];
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

        public int? GetContinuousRangeStart(int num)
        {
            return num switch
            {
                1 => FirstSetBit(),
                < BitsInWord => FindSmallRange(num),
                < 64 => FindLargeRange<int>(num),
                _ => FindLargeRange<long>(num)
            };
        }

        private int? FirstSetBit()
        {
            for (int i = 0; i < _inner.Length; i += Vector256<uint>.Count)
            {
                var a = Vector256.LoadUnsafe(ref _inner[i]).AsUInt32();
                var gt = Vector256.GreaterThan(a, Vector256<uint>.Zero);
                if (gt == Vector256<uint>.Zero)
                    continue;

                var mask = gt.ExtractMostSignificantBits();
                var idx = BitOperations.TrailingZeroCount(mask) + i;
                var item = _inner[idx];
                return idx * BitsInWord + BitOperations.TrailingZeroCount(item);
            }

            return null;
        }

        private int? FindSmallRange(int num)
        {
            // finding sequences up to 32 bits
            for (var i = 0; i < _inner.Length; i++)
            {
                int current = _inner[i];
                if (current == 0)
                    continue;

                if (current == -1)
                    return i * BitsInWord;

                var currentCopy = current;
                int numCopy = num - 1;

                // find consecutive range: https://stackoverflow.com/a/37903049/6366
                // perform AND operations with shifted versions of the number
                // this will leave 1s only where there were n consecutive 1s
                while (currentCopy != 0 && numCopy-- > 0)
                {
                    currentCopy &= (currentCopy << 1);
                }

                if (currentCopy != 0)
                {
                    int position = BitOperations.TrailingZeroCount(currentCopy);
                    return i * BitsInWord + position - (num - 1);
                }

                if (i == _inner.Length - 1)
                {
                    // this is the last word, no next word to check with
                    break;
                }

                // we didn't find the sequence in the word, let's check it between words
                int numberOfSetBitsCurrent = BitOperations.LeadingZeroCount((uint)~current);
                var nextWord = _inner[i + 1];
                var numberOfSetBitsNext = BitOperations.TrailingZeroCount(~nextWord);

                if (numberOfSetBitsCurrent + numberOfSetBitsNext >= num)
                    return (i * BitsInWord) + (BitsInWord - numberOfSetBitsCurrent);
            }

            return null;
        }

        private unsafe int? FindLargeRange<T>(int num)
            where T : unmanaged, INumber<T>
        {
            // finding sequences larger than 32 bits
            // the idea is that we look for sequences that bridge across words using leading/trailing zero counts
            var start = -1;
            var count = 0;

            int currentBitsInWord = sizeof(T) * 8;
            var span = MemoryMarshal.Cast<int, T>(_inner);

            for (var i = 0; i < span.Length; i++)
            {
                T current = span[i];
                if (current == T.Zero)
                {
                    start = -1;
                    count = 0;
                    continue;
                }

                if (current == -T.One)
                {
                    if (start == -1)
                    {
                        start = i * currentBitsInWord;
                    }

                    count += currentBitsInWord;
                    if (count >= num)
                        return start;

                    continue;
                }

                if (start == -1)
                {
                    // find trailing ones at the end of the word if no sequence has started
                    CheckTrailingSequence();
                }
                else
                {
                    if (count + (TotalBits - i * currentBitsInWord) < num)
                    {
                        // impossible to satisfy the continuous bit requirement
                        return null;
                    }

                    if (count + (currentBitsInWord - 1) < num)
                    {
                        // impossible to satisfy the continuous bit requirement in this word
                        CheckTrailingSequence();
                        continue;
                    }

                    // we look at the beginning of the word
                    int numberOfSetBits = current switch
                    {
                        int integer => BitOperations.TrailingZeroCount(~integer),
                        long l => BitOperations.TrailingZeroCount(~l),
                        _ => throw new NotSupportedException()
                    };
                    count += numberOfSetBits;
                    if (count >= num)
                        return start;

                    // reset for the next sequence
                    CheckTrailingSequence();
                }

                void CheckTrailingSequence()
                {
                    int numberOfSetBits = current switch
                    {
                        int integer => BitOperations.LeadingZeroCount(~(uint)integer),
                        long l => BitOperations.LeadingZeroCount(~(ulong)l),
                        _ => throw new NotSupportedException()
                    };
                    if (numberOfSetBits == 0)
                    {
                        start = -1;
                        count = 0;
                    }
                    else
                    {
                        // Calculate the starting bit position in the array
                        start = (i * currentBitsInWord) + (currentBitsInWord - numberOfSetBits);
                        count = numberOfSetBits;
                    }
                }
            }

            return null;
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

        public DynamicJsonValue ToJson(long key, bool hex)
        {
            IEnumerable collection = hex
                ? _inner.Select(x => x.ToString("X"))
                : _inner;

            return new DynamicJsonValue { 
                ["Key"] = key,
                [nameof(SetCount)] = SetCount, 
                ["Data"] = new DynamicJsonArray(collection) 
            };
        }

        public override string ToString()
        {
            return string.Join(", ", _inner);
        }
    }
}
