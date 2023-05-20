using System;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace Voron.Util.Simd;

public struct SortedDifferentials : ISimdTransform
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Vector256<uint> Encode(Vector256<uint> curr, ref Vector256<uint> prev)
        => Delta(curr, ref prev);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Vector256<uint> Decode(Vector256<uint> curr, ref Vector256<uint> prev)
        => PrefixSum(ref curr, ref prev);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector256<uint> Delta(Vector256<uint> curr, ref Vector256<uint> prev)
    {
        if (Avx2.IsSupported)
        {
            var shiftedCur = Avx2.PermuteVar8x32(curr, Vector256.Create(0xffu, 0, 1, 2, 3, 4, 5, 6)) &
                             Vector256.Create(0u, uint.MaxValue, uint.MaxValue, uint.MaxValue, uint.MaxValue, uint.MaxValue, uint.MaxValue, uint.MaxValue);
            var shiftedPrev = Avx2.PermuteVar8x32(prev, Vector256.Create(7u, 0, 0, 0, 0, 0, 0, 0)) &
                              Vector256.Create(uint.MaxValue, 0, 0, 0, 0, 0, 0, 0);
            prev = curr;
            return curr - (shiftedCur | shiftedPrev);
        }
        else
        {
            throw new NotSupportedException("https://github.com/dotnet/runtime/issues/85132");
        }
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector256<uint> PrefixSum(ref Vector256<uint> curr, ref Vector256<uint> prev)
    {
        if (Avx2.IsSupported)
        {
            // see: https://en.algorithmica.org/hpc/algorithms/prefix/
            prev = Avx2.PermuteVar8x32(prev, Vector256.Create(7u, 7, 7, 7, 7, 7, 7, 7));
            curr += Avx2.ShiftLeftLogical128BitLane(curr, 4);
            curr += Avx2.ShiftLeftLogical128BitLane(curr, 8);
            curr += Avx2.PermuteVar8x32(curr, Vector256.Create(0u, 0, 0, 0, 3, 3, 3, 3)) &
                    Vector256.Create(0u, 0, 0, 0, uint.MaxValue, uint.MaxValue, uint.MaxValue, uint.MaxValue);
            var result = curr + prev;
            prev = result;
            return result;
        }
        else
        {
            throw new NotSupportedException("https://github.com/dotnet/runtime/issues/85132");
        }
    }
}
