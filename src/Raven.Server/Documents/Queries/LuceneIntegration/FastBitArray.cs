using System;
using System.Buffers;
using System.Collections.Generic;
using System.Numerics;
using System.Runtime.Intrinsics.X86;

namespace Raven.Server.Documents.Queries.LuceneIntegration
{
    public class FastBitArray : IDisposable
    {
        private ulong[] _bits;
        public FastBitArray(int countOfBits)
        {
            _bits = ArrayPool<ulong>.Shared.Rent(countOfBits / 64 + (countOfBits % 64 == 0 ? 0 : 1));
        }

        public void Set(int index)
        {
            _bits[index / 64] |= 1UL << index % 64;
        }

        public IEnumerable<int> Iterate(int from)
        {
            // https://lemire.me/blog/2018/02/21/iterating-over-set-bits-quickly/
            for (int i = from / 64; i < _bits.Length; i++)
            {
                ulong bitmap = _bits[i];
                while (bitmap != 0)
                {
                    ulong t = bitmap & (ulong)-(long)bitmap;
                    int count = BitOperations.TrailingZeroCount(bitmap);
                    int setBitPos = i * 64 + count;
                    if (setBitPos >= from) 
                        yield return setBitPos; 
                    bitmap ^= t;
                }
            }
        }

        public void Dispose()
        {
            if (_bits == null)
                return;
            ArrayPool<ulong>.Shared.Return(_bits);
            _bits = null;
        }
    }
}
