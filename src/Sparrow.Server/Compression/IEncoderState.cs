using System;
using System.Runtime.CompilerServices;

namespace Sparrow.Server.Compression
{ 
    public interface IEncoderState : IDisposable
    {
        bool CanGrow { get; }

        void Grow(int minimumSize);

        Span<byte> EncodingTable
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get;
        }

        Span<byte> DecodingTable
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get;
        }
    }
}
