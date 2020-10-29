using System;
using System.Buffers;
using System.Collections.Generic;
using System.Numerics;

namespace Raven.Server.Documents.Queries.LuceneIntegration
{
    public struct FastBitArray : IDisposable
    {
        private ulong[] _bits;
        public bool Disposed => _bits == null;
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
            int i = from / 64;
            if (i >= _bits.Length)
                yield break;

            ulong bitmap = _bits[i];
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
                if (i >= _bits.Length)
                    break;
                bitmap = _bits[i];
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
