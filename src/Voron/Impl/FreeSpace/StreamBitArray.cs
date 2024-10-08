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

namespace Voron.Impl.FreeSpace
{
    public sealed class StreamBitArray
    {
        private const int CountOfItems = 64;
        private const int BitsInItem = 32;
        private const int TotalBits = CountOfItems * BitsInItem;

        readonly int[] _inner = new int[CountOfItems];
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

        public int? GetContinuousRangeStart(int num)
        {
            switch (num)
            {
                case 1:
                    // finding a single set bit
                    for (var i = 0; i < _inner.Length; i++)
                    {
                        switch (_inner[i])
                        {
                            case 0:
                                continue;
                            case -1:
                                return i * BitsInItem;
                            default:
                                return i * BitsInItem + BitOperations.TrailingZeroCount(_inner[i]);
                        }
                    }

                    return null;

                case < BitsInItem:
                    // finding sequences up to 32 bits

                    for (var i = 0; i < _inner.Length; i++)
                    {
                        int current = _inner[i];
                        if (current == 0)
                            continue;

                        if (current == -1)
                            return i * BitsInItem;

                        int currentCopy = current;
                        int position = 0;

                        while (currentCopy != 0)
                        {
                            int firstSetBitPos = BitOperations.TrailingZeroCount((uint)currentCopy);
                            position += firstSetBitPos;

                            currentCopy >>= firstSetBitPos;
                            if (currentCopy == -1)
                            {
                                // all bits after the shift are ones
                                if (BitsInItem - position >= num)
                                    return i * BitsInItem + position;

                                break;
                            }

                            int onesCount = BitOperations.TrailingZeroCount((uint)~currentCopy);
                            if (onesCount >= num)
                                return i * BitsInItem + position;

                            position += onesCount;

                            if (BitsInItem - position < num)
                            {
                                // impossible to satisfy the continuous bit requirement
                                break;
                            }

                            currentCopy >>= onesCount;
                            if (currentCopy != 0)
                            {
                                int zerosCount = BitOperations.TrailingZeroCount((uint)currentCopy);
                                position += zerosCount;
                                currentCopy >>= zerosCount; // prepare for the next iteration
                            }
                        }

                        if (i == _inner.Length - 1)
                        {
                            // this is the last block, no next block to check with
                            break;
                        }

                        // we didn't find the sequence in the block, let's check it between blocks
                        int numberOfSetBitsCurrent = BitOperations.LeadingZeroCount((uint)~current);
                        var nextBlock = _inner[i + 1];
                        var numberOfSetBitsNext = BitOperations.TrailingZeroCount(~nextBlock);

                        if (numberOfSetBitsCurrent + numberOfSetBitsNext >= num)
                            return (i * BitsInItem) + (BitsInItem - numberOfSetBitsCurrent);
                    }

                    return null;

                default:
                    // finding sequences larger than 32 bits
                    // the idea is that we look for sequences that bridge across blocks using leading/trailing zero counts
                    var start = -1;
                    var count = 0;

                    for (var i = 0; i < _inner.Length; i++)
                    {
                        int current = _inner[i];
                        if (current == 0)
                        {
                            start = -1;
                            count = 0;
                            continue;
                        }

                        if (current == -1)
                        {
                            if (start == -1)
                            {
                                start = i * BitsInItem;
                            }

                            count += BitsInItem;
                            if (count >= num)
                                return start;

                            continue;
                        }

                        if (start == -1)
                        {
                            // find trailing ones at the end of the block if no sequence has started
                            CheckTrailingSequence();
                        }
                        else
                        {
                            if (count + (TotalBits - i * BitsInItem) < num)
                            {
                                // impossible to satisfy the continuous bit requirement
                                return null;
                            }

                            if (count + 31 < num)
                            {
                                // impossible to satisfy the continuous bit requirement in this block
                                CheckTrailingSequence();
                                continue;
                            }

                            // we look at the beginning of the block
                            int numberOfSetBits = BitOperations.TrailingZeroCount(~current);
                            count += numberOfSetBits;
                            if (count >= num)
                                return start;

                            // reset for the next sequence
                            CheckTrailingSequence();
                        }

                        void CheckTrailingSequence()
                        {
                            int numberOfSetBits = BitOperations.LeadingZeroCount((uint)~current);
                            if (numberOfSetBits == 0)
                            {
                                start = -1;
                                count = 0;
                            }
                            else
                            {
                                // Calculate the starting bit position in the array
                                start = (i * BitsInItem) + (BitsInItem - numberOfSetBits);
                                count = numberOfSetBits;
                            }
                        }
                    }

                    return null;
            }
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
    }
}
