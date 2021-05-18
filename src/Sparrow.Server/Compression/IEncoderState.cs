using System;
using System.Runtime.CompilerServices;

namespace Sparrow.Server.Compression
{ 
    public interface IEncoderState
    {
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
