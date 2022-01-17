using System;
using System.Buffers;
using System.Collections.Generic;
using System.Numerics;
using Sparrow;

namespace Raven.Server.Documents.Queries.LuceneIntegration
{
    public struct FastBitArray 
    {
        public ulong[] Bits;
        public bool Disposed => Bits == null;
        public Size Size => new Size(Bits.Length * sizeof(long), SizeUnit.Bytes);

        public FastBitArray(int countOfBits)
        {
            Bits = ArrayPool<ulong>.Shared.Rent(countOfBits / 64 + (countOfBits % 64 == 0 ? 0 : 1));
            new Span<ulong>(Bits).Clear();
        }

        public FastBitArray(ulong[] bits)
        {
            Bits = bits;
        }
        
        public void Set(int index)
        {
            Bits[index / 64] |= 1UL << index % 64;
        }

        public int IndexOfFirstSetBit()
        {
            for (int i = 0; i < Bits.Length; i++)
            {
                if (Bits[i] == 0) 
                    continue;
                
                int count = BitOperations.TrailingZeroCount(Bits[i]);
                return i * 64 + count;
            }

            return -1;
        }

        public IEnumerable<int> Iterate(int from)
        {
            // https://lemire.me/blog/2018/02/21/iterating-over-set-bits-quickly/
            int i = from / 64;
            if (i >= Bits.Length)
                yield break;

            ulong bitmap = Bits[i];
            bitmap &= ulong.MaxValue << (from % 64);
            while (true)
            {
                while (bitmap != 0)
                {
                    ulong t = bitmap & (ulong)-(long)bitmap;
                    int count = BitOperations.TrailingZeroCount(bitmap);
                    int setBitPos = i * 64 + count;
                    yield return setBitPos; 
                    bitmap ^= t;
                }
                i++;
                if (i >= Bits.Length)
                    break;
                bitmap = Bits[i];
            }
        }
    }
}
