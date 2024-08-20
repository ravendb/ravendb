using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

#if NET6_0_OR_GREATER     
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.Arm;
using System.Runtime.Intrinsics.X86;
#endif

namespace Sparrow
{
    internal static class PortableIntrinsics
    {

#if NET6_0_OR_GREATER
        /// <summary>
        /// Computes a mask from the most significant bits of the bytes in the input vector.
        /// The mask is formed by taking the most significant bit of each byte in the input vector
        /// and setting the corresponding bit in the result. 
        /// </summary>
        /// <param name="input">The input vector of bytes.</param>
        /// <returns>An integer where each bit represents the most significant bit of the corresponding byte in the input vector.</returns>
        /// <exception cref="NotSupportedException">Thrown if neither SSE2 nor AdvSimd is supported on the current architecture.</exception>
        /// <remarks>
        /// This method relies on specific CPU instructions for efficient computation of the mask:
        /// 1. If AVX2 is supported, the __mm256_movemask_epi8 method is used.
        /// 2. If AdvSimd (Advanced SIMD) is supported, the emulation method is used.
        /// 
        /// Assumptions and Limitations:
        /// - The input vector is assumed to be 256 bits (32 bytes) long. 
        /// - The implementation for AdvSimd is limited by the vector size and performance of pairwise
        ///   widening operations which might not be optimal for all architectures
        /// - While it could be possible to avoid the usage of the shift operation, we are looking into a portable
        ///   version with the same behavior on Intel and ARM; even if it is at the expense of performance in this case.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int MoveMask(in Vector256<byte> input)
        {
            if (Avx2.IsSupported)
                return Avx2.MoveMask(input);
            
            if (AdvSimd.IsSupported)
                return MoveMaskAdvSimd(in input);
            
            throw new NotSupportedException($"{nameof(MoveMask)} is not supported on this architecture");
        }

        internal static int MoveMaskAdvSimd(in Vector256<byte> input)
        {
            Debug.Assert(AdvSimd.IsSupported);
            
            var upper = input.GetUpper();
            var lower = input.GetLower();

            // Because the Intel version uses the high bit, we need to `Vector.And` and then `Vector.ShiftLogical`. 
            // PERF: For high performance code that really requires to get rid of the shift operation we could implement
            //       an `.MoveMaskUnsafe()` version assuming only 0x00 and 0xFF values are valid which allow us to just
            //       get the proper bit directly. 
            var upperMasked = AdvSimd.ShiftLogical(
                AdvSimd.And(upper, Vector128.Create((byte)128)),
                Shift128);
            var lowerMasked = AdvSimd.ShiftLogical(
                AdvSimd.And(lower, Vector128.Create((byte)128)),
                Shift128);

            var upperReduced8 = AdvSimd.AddPairwiseWidening(upperMasked);
            var upperReduced16 = AdvSimd.AddPairwiseWidening(upperReduced8.AsUInt16());
            var upperReduced32 = AdvSimd.AddPairwiseWidening(upperReduced16.AsUInt32());

            var lowerReduced8 = AdvSimd.AddPairwiseWidening(lowerMasked);
            var lowerReduced16 = AdvSimd.AddPairwiseWidening(lowerReduced8.AsUInt16());
            var lowerReduced32 = AdvSimd.AddPairwiseWidening(lowerReduced16.AsUInt32());

            ulong output = 0;
            output |= lowerReduced32.ToScalar();
            output |= lowerReduced32.GetElement(1) << 8;
            output |= upperReduced32.ToScalar() << 16;
            output |= upperReduced32.GetElement(1) << 24;
            return (int)output;
        }

        /// <summary>
        /// Computes a mask from the most significant bits of the bytes in the input vector.
        /// The mask is formed by taking the most significant bit of each byte in the input vector
        /// and setting the corresponding bit in the result.
        /// </summary>
        /// <param name="input">The input vector of bytes.</param>
        /// <returns>An integer where each bit represents the most significant bit of the corresponding byte in the input vector.</returns>
        /// <exception cref="NotSupportedException">Thrown if neither SSE2 nor AdvSimd is supported on the current architecture.</exception>
        /// <remarks>
        /// This method relies on specific CPU instructions for efficient computation of the mask:
        /// 1. If SSE2 is supported, the __mm128_movemask_epi8 method is used.
        /// 2. If AdvSimd (Advanced SIMD) is supported, the emulation method is used.
        /// 
        /// Assumptions and Limitations:
        /// - The input vector is assumed to be 128 bits (16 bytes) long. 
        /// - The implementation for AdvSimd is limited by the availability and performance of pairwise
        ///   widening operations which might not be optimal for all architectures.
        /// - While it could be possible to avoid the usage of the shift operation, we are looking into a portable
        ///   version with the same behavior on Intel and ARM; even if it is at the expense of performance in this case.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int MoveMask(in Vector128<byte> input)
        {
            if (Sse2.IsSupported)
                return Sse2.MoveMask(input);

            if (AdvSimd.IsSupported)
                return MoveMaskAdvSimd(in input);

            throw new NotSupportedException($"{nameof(MoveMask)} is not supported on this architecture");
        }

        private static readonly Vector128<sbyte> Shift128 = Vector128.Create(-7, -6, -5, -4, -3, -2, -1, 0, -7, -6, -5, -4, -3, -2, -1, 0);

        internal static int MoveMaskAdvSimd(in Vector128<byte> input)
        {
            Debug.Assert(AdvSimd.IsSupported);

            // Because the Intel version uses the high bit, we need to `Vector.And` and then `Vector.ShiftLogical`. 
            // PERF: For high performance code that really requires to get rid of the shift operation we could implement
            //       an `.MoveMaskUnsafe()` version assuming only 0x00 and 0xFF values are valid which allow us to just
            //       get the proper bit directly. 
            var masked = AdvSimd.ShiftLogical(
                AdvSimd.And(input, Vector128.Create((byte)128)),
                Shift128);

            var reduced8 = AdvSimd.AddPairwiseWidening(masked);
            var reduced16 = AdvSimd.AddPairwiseWidening(reduced8.AsUInt16());
            var reduced32 = AdvSimd.AddPairwiseWidening(reduced16.AsUInt32());

            ulong output = 0;
            output |= reduced32.ToScalar();
            output |= reduced32.GetElement(1) << 8;
            return (int)output;
        }
#endif


    }
}
