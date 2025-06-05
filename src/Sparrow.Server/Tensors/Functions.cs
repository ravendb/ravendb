using System;
using System.Diagnostics;
using System.Net;
using System.Numerics;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;
using System.Runtime.Intrinsics;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.Arm;
using Sparrow.Platform;

namespace Sparrow.Server.Tensors
{
    public static class Functions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T CosineDistance<T>(ReadOnlySpan<T> a, ReadOnlySpan<T> b)
            where T : unmanaged, IFloatingPoint<T>, IRootFunctions<T>, INumber<T>
        {
            return T.One - CosineSimilarity(a, b);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static TResult CosineDistance<T, TResult>(ReadOnlySpan<T> a, ReadOnlySpan<T> b)
            where T : unmanaged, IRootFunctions<T>, INumber<T>
            where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
        {
            return TResult.One - CosineSimilarity<T, TResult>(a, b);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T CosineSimilarity<T>(ReadOnlySpan<T> a, ReadOnlySpan<T> b)
            where T : unmanaged, IFloatingPoint<T>, IRootFunctions<T>, INumber<T>
        {
            if (PlatformDetails.Is32Bits)
                return TensorPrimitives.CosineSimilarity(a, b);
            
            if (a.Length >= Vector512<T>.Count)
            {
                if (typeof(T) == typeof(float))
                {
                    if (AdvInstructionSet.IsAcceleratedVector256)
                    {
                        return Vectorized512.CosineSimilarity<T, T>(a, b);
                    }

                    return TensorPrimitives.CosineSimilarity(a, b);
                }

                if (typeof(T) == typeof(double) && AdvInstructionSet.IsAcceleratedVector256)
                {
                    return Vectorized512.CosineSimilarity<T, T>(a, b);
                }
            }

            return Serial.CosineSimilarity<T,T>(a, b);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static TResult CosineSimilarity<T, TResult>(ReadOnlySpan<T> a, ReadOnlySpan<T> b)
            where T : unmanaged, INumber<T>
            where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
        {
            if (typeof(T) == typeof(sbyte))
            {
                if (a.Length >= Vector256<sbyte>.Count && AdvInstructionSet.X86.IsSupportedAvx256)
                {
                    if (Avx512BW.IsSupported && Avx512F.IsSupported)
                    {
                        return Vectorized512.CosineSimilarityIntegersAvx512(
                            MemoryMarshal.Cast<T, sbyte>(a), TResult.One,
                            MemoryMarshal.Cast<T, sbyte>(b), TResult.One);
                    }

                    return Vectorized256.CosineSimilarityIntegersAvx2(
                        MemoryMarshal.Cast<T, sbyte>(a), TResult.One,
                        MemoryMarshal.Cast<T, sbyte>(b), TResult.One);
                }

                if (a.Length >= Vector256<sbyte>.Count && AdvInstructionSet.Arm.IsSupported && Dp.IsSupported)
                {
                    return Vectorized256.CosineSimilarityIntegersNeon(
                        MemoryMarshal.Cast<T, sbyte>(a), TResult.One,
                        MemoryMarshal.Cast<T, sbyte>(b), TResult.One);
                }
            }

            if (a.Length >= Vector512<T>.Count && AdvInstructionSet.IsAcceleratedVector256)
            {
                if (typeof(T) == typeof(float))
                {
                    return Vectorized512.CosineSimilarity<float, TResult>(
                        MemoryMarshal.Cast<T, float>(a),
                        MemoryMarshal.Cast<T, float>(b));
                }

                if (typeof(T) == typeof(double))
                {
                    return Vectorized512.CosineSimilarity<double, TResult>(
                        MemoryMarshal.Cast<T, double>(a),
                        MemoryMarshal.Cast<T, double>(b));
                }
            }

            return Serial.CosineSimilarity<T, TResult>(a, b);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static TResult CosineSimilarity<T, TResult>(ReadOnlySpan<T> a, TResult aMagnitude, ReadOnlySpan<T> b, TResult bMagnitude)
            where T : unmanaged, INumber<T>
            where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
        {
            if (typeof(T) == typeof(sbyte))
            {
                if (a.Length >= Vector256<sbyte>.Count && AdvInstructionSet.X86.IsSupportedAvx256)
                {
                    if (AdvInstructionSet.X86.IsSupportedAvx512Basic)
                    {
                        return Vectorized512.CosineSimilarityIntegersAvx512(
                            MemoryMarshal.Cast<T, sbyte>(a), aMagnitude,
                            MemoryMarshal.Cast<T, sbyte>(b), bMagnitude);
                    }

                    return Vectorized256.CosineSimilarityIntegersAvx2(
                        MemoryMarshal.Cast<T, sbyte>(a), aMagnitude,
                        MemoryMarshal.Cast<T, sbyte>(b), bMagnitude);
                }

                if (a.Length >= Vector256<sbyte>.Count && AdvInstructionSet.Arm.IsSupported && Dp.IsSupported)
                {
                    return Vectorized256.CosineSimilarityIntegersNeon(
                        MemoryMarshal.Cast<T, sbyte>(a), aMagnitude,
                        MemoryMarshal.Cast<T, sbyte>(b), bMagnitude);
                }
            }

            if (a.Length >= Vector512<T>.Count && AdvInstructionSet.IsAcceleratedVector256)
            {
                if (typeof(T) == typeof(float))
                {
                    return Vectorized512.CosineSimilarity<float, TResult>(
                        MemoryMarshal.Cast<T, float>(a), aMagnitude,
                        MemoryMarshal.Cast<T, float>(b), bMagnitude);
                }

                if (typeof(T) == typeof(double))
                {
                    return Vectorized512.CosineSimilarity<double, TResult>(
                        MemoryMarshal.Cast<T, double>(a), aMagnitude,
                        MemoryMarshal.Cast<T, double>(b), bMagnitude);
                }
            }

            return Serial.CosineSimilarity<T, TResult>(a, aMagnitude, b, bMagnitude);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static TResult CosineDistance<T, TResult>(ReadOnlySpan<T> a, TResult aMagnitude, ReadOnlySpan<T> b, TResult bMagnitude)
            where T : unmanaged, INumber<T>
            where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
        {
            return TResult.One - CosineSimilarity(a, aMagnitude, b, bMagnitude);
        }

        public static class Serial
        {
            [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
            internal static TResult CosineSimilarityNormalize<T, TResult>(T ab, T a2, T b2)
                where T : unmanaged, IRootFunctions<T>, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                // Convert the accumulation values from T to TResult.
                // This assumes that converting from T to TResult is lossless or acceptable in your context.
                TResult a2Conv = TResult.CreateTruncating(a2);
                TResult b2Conv = TResult.CreateTruncating(b2);
                TResult abConv = TResult.CreateTruncating(ab);

                // Compute the reciprocal of the magnitudes:
                // invSqrtA2 = 1 / sqrt(a2) and invSqrtB2 = 1 / sqrt(b2)
                TResult invSqrtA2B2 = TResult.Sqrt(a2Conv) * TResult.Sqrt(b2Conv);

                // Calculate the cosine similarity (note that cos(theta) = ab / (sqrt(a2)*sqrt(b2)))
                TResult cosineSimilarity = abConv / invSqrtA2B2;
                return cosineSimilarity;
            }

            /// <summary>
            /// Serial implementation of Cosine distance.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
            public static TResult CosineSimilarity<T, TResult>(ReadOnlySpan<T> a, ReadOnlySpan<T> b)
                where T : unmanaged, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                TResult ab = TResult.Zero, a2 = TResult.Zero, b2 = TResult.Zero;
                for (int i = 0; i < a.Length; i++)
                {
                    var rai = TResult.CreateTruncating(a[i]);
                    var rbi = TResult.CreateTruncating(b[i]);
                    ab += rai * rbi;
                    a2 += rai * rai;
                    b2 += rbi * rbi;
                }

                // Special cases
                if (TResult.IsZero(a2) && TResult.IsZero(b2))
                    return TResult.CreateTruncating(double.NaN); // Both zero vectors: nan
                if (TResult.IsZero(ab))
                    return TResult.Zero; // Orthogonal or one zero: distance = 1, similarity 0

                // Normalization
                return CosineSimilarityNormalize<TResult, TResult>(ab, a2, b2);
            }

            [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
            public static TResult CosineSimilarity<T, TResult>(ReadOnlySpan<T> a, TResult aMagnitude, ReadOnlySpan<T> b, TResult bMagnitude)
                where T : unmanaged, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                if (typeof(T) == typeof(float) || typeof(T) == typeof(double))
                {
                    return CosineSimilarityFloatingPoint(a, aMagnitude, b, bMagnitude);
                }

                return CosineSimilarityIntegers(a, aMagnitude, b, bMagnitude);
            }

            [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
            private static TResult CosineSimilarityIntegers<T, TResult>(ReadOnlySpan<T> a, TResult aMagnitude, ReadOnlySpan<T> b, TResult bMagnitude)
                where T : unmanaged, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                long ab = 0, a2 = 0, b2 = 0;
                for (int i = 0; i < a.Length; i++)
                {
                    long rai = long.CreateTruncating(a[i]);
                    long rbi = long.CreateTruncating(b[i]);

                    ab += rai * rbi;
                    a2 += rai * rai;
                    b2 += rbi * rbi;
                }

                // Special cases
                if (a2 == 0 && b2 == 0)
                    return TResult.CreateTruncating(double.NaN); // Both zero vectors: nan
                if (ab == 0)
                    return TResult.Zero; // Orthogonal or one zero: distance = 1, similarity 0

                // Normalization
                TResult fab = aMagnitude * bMagnitude * TResult.CreateTruncating(ab);
                TResult fa2 = aMagnitude * aMagnitude * TResult.CreateTruncating(a2);
                TResult fb2 = bMagnitude * bMagnitude * TResult.CreateTruncating(b2);
                return CosineSimilarityNormalize<TResult, TResult>(fab, fa2, fb2);
            }

            [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
            public static TResult CosineSimilarityFloatingPoint<T, TResult>(ReadOnlySpan<T> a, TResult aMagnitude, ReadOnlySpan<T> b, TResult bMagnitude)
                where T : unmanaged, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                TResult ab = TResult.Zero, a2 = TResult.Zero, b2 = TResult.Zero;
                for (int i = 0; i < a.Length; i++)
                {
                    ab += TResult.CreateTruncating(a[i] * b[i]);
                    a2 += TResult.CreateTruncating(a[i] * a[i]);
                    b2 += TResult.CreateTruncating(b[i] * b[i]);
                }

                // Special cases
                if (a2 == TResult.Zero && b2 == TResult.Zero)
                    return TResult.CreateTruncating(double.NaN); // Both zero vectors: nan
                if (ab == TResult.Zero)
                    return TResult.Zero; // Orthogonal or one zero: distance = 1, similarity 0

                // Normalization
                TResult fab = aMagnitude * bMagnitude * TResult.CreateTruncating(ab);
                TResult fa2 = aMagnitude * aMagnitude * TResult.CreateTruncating(a2);
                TResult fb2 = bMagnitude * bMagnitude * TResult.CreateTruncating(b2);
                return CosineSimilarityNormalize<TResult, TResult>(fab, fa2, fb2);
            }
        }

        public static class Vectorized512
        {
            private static ReadOnlySpan<byte> MoveMaskTable =>
            [
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 64 bits
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 128 bits
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 256 bits
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 512 bits
                // Now comes the part were we are having 0xFF
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 64 bits
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 128 bits
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 256 bits
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, //
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 512 bits
            ];

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static double CosineSimilarityInternalX64(ref float aRef, float aMagnitude, ref float bRef, float bMagnitude, nuint size)
            {
                Vector512<float> abVec = Vector512<float>.Zero;
                Vector512<float> a2Vec = Vector512<float>.Zero;
                Vector512<float> b2Vec = Vector512<float>.Zero;

                nuint i = 0;
                nuint oneVectorFromEnd = size - (nuint)Vector512<float>.Count;

            Loop:

                // PERF: The reason why this would work on hardware not supporting 512-bit vectors is
                // that it will effectively create 2 lanes (xmm and ymm) of 256-bit vectors. And because
                // there are no overlapping lanes, there will be less pipeline dependencies hiding latency
                // of the instructions themselves.
                Vector512<float> aVec = Vector512.LoadUnsafe(ref aRef, i);
                Vector512<float> bVec = Vector512.LoadUnsafe(ref bRef, i);

                i += (nuint)Vector512<float>.Count;

            LoopWithoutLoad:

                abVec = Arithmetics.MultiplyAddEstimate(aVec, bVec, abVec);
                a2Vec = Arithmetics.MultiplyAddEstimate(aVec, aVec, a2Vec);
                b2Vec = Arithmetics.MultiplyAddEstimate(bVec, bVec, b2Vec);

                if (i <= oneVectorFromEnd)
                    goto Loop;

                if (i != (nuint)size)
                {
                    nuint offset = size - i;
                    Debug.Assert((int)offset * sizeof(float) + Vector512<byte>.Count <= MoveMaskTable.Length);

                    var mask = Vector512.LoadUnsafe<float>(
                        ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, float>(MoveMaskTable)), (nuint)offset);

                    aVec = Vector512.BitwiseAnd(Vector512.LoadUnsafe(ref aRef, oneVectorFromEnd), mask);
                    bVec = Vector512.BitwiseAnd(Vector512.LoadUnsafe(ref bRef, oneVectorFromEnd), mask);

                    i = (nuint)size;
                    goto LoopWithoutLoad;
                }

                float ab = aMagnitude * bMagnitude * Vector512.Sum(abVec);
                float a2 = aMagnitude * aMagnitude * Vector512.Sum(a2Vec);
                float b2 = bMagnitude * bMagnitude * Vector512.Sum(b2Vec);

                // Special cases
                if (a2 == 0 && b2 == 0)
                    return double.NaN; // Both zero vectors: nan
                if (ab == 0)
                    return 0; // Orthogonal or one zero: distance = 1, similarity 0

                // Normalization
                // Create a 128-bit vector with a2 in the high lane and b2 in the low lane.
                // Note: _mm_set_pd(a2, b2) in C sets lane1=a2 and lane0=b2.
                // In .NET, Vector128.Create(x, y) sets lane0 = x and lane1 = y.
                // So we swap the order.
                var squares = Vector128.Create((double)b2, (double)a2);

                // Compute approximate reciprocal square root (single precision).
                var rsqrts = Sse2.ConvertToVector128Double(
                    Sse.ReciprocalSqrt(
                        Sse2.ConvertToVector128Single(squares))
                );

                // Newton-Raphson iteration for reciprocal square root:
                // https://en.wikipedia.org/wiki/Newton%27s_method
                rsqrts = Sse2.Add(
                    Sse2.Multiply(Vector128.Create(1.5d), rsqrts),
                    Sse2.Multiply(
                        Sse2.Multiply(
                            Sse2.Multiply(squares, Vector128.Create(-0.5d)),
                            rsqrts),
                        Sse2.Multiply(rsqrts, rsqrts)
                    )
                );

                // Extract the results.
                // According to our lane ordering:
                //   - Lane 0 contains b2 reciprocal.
                //   - Lane 1 contains a2 reciprocal.
                double b2Reciprocal = rsqrts.ToScalar(); // lane 0
                double a2Reciprocal = Sse2.UnpackHigh(rsqrts, rsqrts).ToScalar(); // lane 1
                return  ab * a2Reciprocal * b2Reciprocal;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static double CosineSimilarityInternalNeon(ref float aRef, float aMagnitude, ref float bRef, float bMagnitude, nuint size)
            {
                Vector512<float> abVec = Vector512<float>.Zero;
                Vector512<float> a2Vec = Vector512<float>.Zero;
                Vector512<float> b2Vec = Vector512<float>.Zero;

                nuint i = 0;
                nuint oneVectorFromEnd = size - (nuint)Vector512<float>.Count;

            Loop:

                // PERF: The reason why this would work on hardware not supporting 512-bit vectors is
                // that it will effectively create 2 lanes (xmm and ymm) of 256-bit vectors. And because
                // there are no overlapping lanes, there will be less pipeline dependencies hiding latency
                // of the instructions themselves.
                Vector512<float> aVec = Vector512.LoadUnsafe(ref aRef, i);
                Vector512<float> bVec = Vector512.LoadUnsafe(ref bRef, i);

                i += (nuint)Vector512<float>.Count;

            LoopWithoutLoad:

                abVec = Arithmetics.MultiplyAddEstimate(aVec, bVec, abVec);
                a2Vec = Arithmetics.MultiplyAddEstimate(aVec, aVec, a2Vec);
                b2Vec = Arithmetics.MultiplyAddEstimate(bVec, bVec, b2Vec);

                if (i <= oneVectorFromEnd)
                    goto Loop;

                if (i != (nuint)size)
                {
                    nuint offset = size - i;
                    Debug.Assert((int)offset * sizeof(float) + Vector512<byte>.Count <= MoveMaskTable.Length);

                    var mask = Vector512.LoadUnsafe<float>(
                        ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, float>(MoveMaskTable)), (nuint)offset);

                    aVec = Vector512.BitwiseAnd(Vector512.LoadUnsafe(ref aRef, oneVectorFromEnd), mask);
                    bVec = Vector512.BitwiseAnd(Vector512.LoadUnsafe(ref bRef, oneVectorFromEnd), mask);

                    i = (nuint)size;
                    goto LoopWithoutLoad;
                }

                float ab = aMagnitude * bMagnitude * Vector512.Sum(abVec);
                float a2 = aMagnitude * aMagnitude * Vector512.Sum(a2Vec);
                float b2 = bMagnitude * bMagnitude * Vector512.Sum(b2Vec);

                // Special cases
                if (a2 == 0 && b2 == 0)
                    return double.NaN; // Both zero vectors: nan
                if (ab == 0)
                    return 0; // Orthogonal or one zero: distance = 1, similarity 0

                // Create vector with the squared magnitudes
                var squares = Vector64.Create(float.CreateTruncating(b2), float.CreateTruncating(a2));

                // Compute reciprocal square root approximation
                Vector64<float> rsqrts = AdvSimd.ReciprocalSquareRootEstimate(squares);

                // Perform two rounds of Newton-Raphson refinement for better accuracy
                // Formula: rsqrt_new = rsqrt * (1.5 - 0.5 * x * rsqrt^2)
                // Which can be rewritten as: rsqrt * vrsqrts(x * rsqrt, rsqrt)
                rsqrts = AdvSimd.Multiply(rsqrts, AdvSimd.ReciprocalSquareRootStep(AdvSimd.Multiply(squares, rsqrts), rsqrts));
                rsqrts = AdvSimd.Multiply(rsqrts, AdvSimd.ReciprocalSquareRootStep(AdvSimd.Multiply(squares, rsqrts), rsqrts));

                // Extract the refined reciprocal square roots
                float rsqrtA = rsqrts.GetElement(0);
                float rsqrtB = rsqrts.GetElement(1);

                // Compute the cosine similarity
                return ab * rsqrtA * rsqrtB;
            }


            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static double CosineSimilarityInternalX64(ref double aRef, double aMagnitude, ref double bRef, double bMagnitude, nuint size)
            {
                Vector512<double> abVec = Vector512<double>.Zero;
                Vector512<double> a2Vec = Vector512<double>.Zero;
                Vector512<double> b2Vec = Vector512<double>.Zero;

                nuint i = 0;
                nuint oneVectorFromEnd = size - (nuint)Vector512<double>.Count;

            Loop:

                // PERF: The reason why this would work on hardware not supporting 512-bit vectors is
                // that it will effectively create 2 lanes (xmm and ymm) of 256-bit vectors. And because
                // there are no overlapping lanes, there will be less pipeline dependencies hiding latency
                // of the instructions themselves.
                Vector512<double> aVec = Vector512.LoadUnsafe(ref aRef, i);
                Vector512<double> bVec = Vector512.LoadUnsafe(ref bRef, i);

                i += (nuint)Vector512<double>.Count;

            LoopWithoutLoad:

                abVec = Arithmetics.MultiplyAddEstimate(aVec, bVec, abVec);
                a2Vec = Arithmetics.MultiplyAddEstimate(aVec, aVec, a2Vec);
                b2Vec = Arithmetics.MultiplyAddEstimate(bVec, bVec, b2Vec);

                if (i <= oneVectorFromEnd)
                    goto Loop;

                if (i != (nuint)size)
                {
                    nuint offset = (nuint)size - i;
                    Debug.Assert((int)offset * sizeof(double) + Vector512<byte>.Count <= MoveMaskTable.Length);

                    var mask = Vector512.LoadUnsafe<double>(
                        ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, double>(MoveMaskTable)), (nuint)offset);

                    aVec = Vector512.BitwiseAnd(Vector512.LoadUnsafe(ref aRef, oneVectorFromEnd), mask);
                    bVec = Vector512.BitwiseAnd(Vector512.LoadUnsafe(ref bRef, oneVectorFromEnd), mask);

                    i = (nuint)size;
                    goto LoopWithoutLoad;
                }

                double ab = aMagnitude * bMagnitude * Vector512.Sum(abVec);
                double a2 = aMagnitude * aMagnitude * Vector512.Sum(a2Vec);
                double b2 = bMagnitude * bMagnitude * Vector512.Sum(b2Vec);

                // Special cases
                if (a2 == 0 && b2 == 0)
                    return double.NaN; // Both zero vectors: nan
                if (ab == 0)
                    return 0; // Orthogonal or one zero: distance = 1, similarity 0

                // Normalization
                // Create a 128-bit vector with a2 in the high lane and b2 in the low lane.
                // Note: _mm_set_pd(a2, b2) in C sets lane1=a2 and lane0=b2.
                // In .NET, Vector128.Create(x, y) sets lane0 = x and lane1 = y.
                // So we swap the order.
                var squares = Vector128.Create((double)b2, (double)a2);

                // Compute approximate reciprocal square root (single precision).
                var rsqrts = Sse2.ConvertToVector128Double(
                    Sse.ReciprocalSqrt(
                        Sse2.ConvertToVector128Single(squares))
                );

                // Newton-Raphson iteration for reciprocal square root:
                // https://en.wikipedia.org/wiki/Newton%27s_method
                rsqrts = Sse2.Add(
                    Sse2.Multiply(Vector128.Create(1.5d), rsqrts),
                    Sse2.Multiply(
                        Sse2.Multiply(
                            Sse2.Multiply(squares, Vector128.Create(-0.5d)),
                            rsqrts),
                        Sse2.Multiply(rsqrts, rsqrts)
                    )
                );

                // Extract the results.
                // According to our lane ordering:
                //   - Lane 0 contains b2 reciprocal.
                //   - Lane 1 contains a2 reciprocal.
                double b2Reciprocal = rsqrts.ToScalar(); // lane 0
                double a2Reciprocal = Sse2.UnpackHigh(rsqrts, rsqrts).ToScalar(); // lane 1
                return ab * a2Reciprocal * b2Reciprocal;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static double CosineSimilarityInternalNeon(ref double aRef, double aMagnitude, ref double bRef, double bMagnitude, nuint size)
            {
                Vector512<double> abVec = Vector512<double>.Zero;
                Vector512<double> a2Vec = Vector512<double>.Zero;
                Vector512<double> b2Vec = Vector512<double>.Zero;

                nuint i = 0;
                nuint oneVectorFromEnd = size - (nuint)Vector512<double>.Count;

            Loop:

                // PERF: The reason why this would work on hardware not supporting 512-bit vectors is
                // that it will effectively create 2 lanes (xmm and ymm) of 256-bit vectors. And because
                // there are no overlapping lanes, there will be less pipeline dependencies hiding latency
                // of the instructions themselves.
                Vector512<double> aVec = Vector512.LoadUnsafe(ref aRef, i);
                Vector512<double> bVec = Vector512.LoadUnsafe(ref bRef, i);

                i += (nuint)Vector512<double>.Count;

            LoopWithoutLoad:

                abVec = Arithmetics.MultiplyAddEstimate(aVec, bVec, abVec);
                a2Vec = Arithmetics.MultiplyAddEstimate(aVec, aVec, a2Vec);
                b2Vec = Arithmetics.MultiplyAddEstimate(bVec, bVec, b2Vec);

                if (i <= oneVectorFromEnd)
                    goto Loop;

                if (i != (nuint)size)
                {
                    nuint offset = (nuint)size - i;
                    Debug.Assert((int)offset * sizeof(double) + Vector512<byte>.Count <= MoveMaskTable.Length);

                    var mask = Vector512.LoadUnsafe<double>(
                        ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, double>(MoveMaskTable)), (nuint)offset);

                    aVec = Vector512.BitwiseAnd(Vector512.LoadUnsafe(ref aRef, oneVectorFromEnd), mask);
                    bVec = Vector512.BitwiseAnd(Vector512.LoadUnsafe(ref bRef, oneVectorFromEnd), mask);

                    i = (nuint)size;
                    goto LoopWithoutLoad;
                }

                double ab = aMagnitude * bMagnitude * Vector512.Sum(abVec);
                double a2 = aMagnitude * aMagnitude * Vector512.Sum(a2Vec);
                double b2 = bMagnitude * bMagnitude * Vector512.Sum(b2Vec);

                // Special cases
                if (a2 == 0 && b2 == 0)
                    return double.NaN; // Both zero vectors: nan
                if (ab == 0)
                    return 0; // Orthogonal or one zero: distance = 1, similarity 0

                // Create vector with the squared magnitudes
                var squares = Vector64.Create(float.CreateTruncating(b2), float.CreateTruncating(a2));

                // Compute reciprocal square root approximation
                Vector64<float> rsqrts = AdvSimd.ReciprocalSquareRootEstimate(squares);

                // Perform two rounds of Newton-Raphson refinement for better accuracy
                // Formula: rsqrt_new = rsqrt * (1.5 - 0.5 * x * rsqrt^2)
                // Which can be rewritten as: rsqrt * vrsqrts(x * rsqrt, rsqrt)
                rsqrts = AdvSimd.Multiply(rsqrts, AdvSimd.ReciprocalSquareRootStep(AdvSimd.Multiply(squares, rsqrts), rsqrts));
                rsqrts = AdvSimd.Multiply(rsqrts, AdvSimd.ReciprocalSquareRootStep(AdvSimd.Multiply(squares, rsqrts), rsqrts));

                // Extract the refined reciprocal square roots
                float rsqrtA = rsqrts.GetElement(0);
                float rsqrtB = rsqrts.GetElement(1);

                // Compute the cosine similarity
                return ab * rsqrtA * rsqrtB;
            }


            [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
            public static TResult CosineSimilarity<T, TResult>(ReadOnlySpan<T> a, ReadOnlySpan<T> b)
                where T : unmanaged, IFloatingPoint<T>, IRootFunctions<T>, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                if (a.Length < Vector512<T>.Count)
                    return Serial.CosineSimilarity<T, TResult>(a, b);

                double similarity;
                if (typeof(T) == typeof(float))
                {
                    ref var afRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<T, float>(a));
                    ref var bfRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<T, float>(b));

                    if (AdvInstructionSet.X86.IsSupportedSse)
                    {
                        similarity = CosineSimilarityInternalX64(ref afRef, 1.0f, ref bfRef, 1.0f, (nuint)a.Length);
                    }
                    else if (AdvInstructionSet.Arm.IsSupported)
                    {
                        similarity = CosineSimilarityInternalNeon(ref afRef, 1.0f, ref bfRef, 1.0f, (nuint)a.Length);
                    }
                    else throw new NotSupportedException($"Type {typeof(T).Name} is not supported.");
                } 
                else if (typeof(T) == typeof(double))
                {
                    ref var adRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<T, double>(a));
                    ref var bdRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<T, double>(b));

                    if (Sse2.IsSupported)
                    {
                        similarity = CosineSimilarityInternalX64(ref adRef, 1.0d, ref bdRef, 1.0d, (nuint)a.Length);
                    }
                    else if (AdvInstructionSet.Arm.IsSupported)
                    {
                        similarity = CosineSimilarityInternalNeon(ref adRef, 1.0d, ref bdRef, 1.0d, (nuint)a.Length);
                    }
                    else throw new NotSupportedException($"Type {typeof(T).Name} is not supported.");
                }
                else throw new NotSupportedException($"Type {typeof(T).Name} is not supported.");

                if (typeof(TResult) == typeof(float))
                {
                    return (TResult)(object)(float)similarity;
                }

                return (TResult)(object)similarity;
            }

            [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
            public static TResult CosineSimilarity<T, TResult>(ReadOnlySpan<T> a, TResult aMagnitude, ReadOnlySpan<T> b, TResult bMagnitude)
                where T : unmanaged, IFloatingPoint<T>, IRootFunctions<T>, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                if (a.Length < Vector512<T>.Count)
                    return Serial.CosineSimilarity<T, TResult>(a, b);

                double similarity;
                if (typeof(T) == typeof(float))
                {
                    float aMag, bMag;
                    if (typeof(TResult) == typeof(float))
                    {
                        aMag = (float)(object)aMagnitude;
                        bMag = (float)(object)bMagnitude;
                    }
                    else if (typeof(TResult) == typeof(double))
                    {
                        aMag = (float)(double)(object)aMagnitude;
                        bMag = (float)(double)(object)bMagnitude;
                    }
                    else throw new NotSupportedException($"Type {typeof(T).Name} is not supported.");

                    ref var adRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<T, float>(a));
                    ref var bdRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<T, float>(b));

                    if (Sse2.IsSupported)
                    {
                        similarity = CosineSimilarityInternalX64(ref adRef, aMag, ref bdRef, bMag, (nuint)a.Length);
                    }
                    else if (AdvInstructionSet.Arm.IsSupported)
                    {
                        similarity = CosineSimilarityInternalNeon(ref adRef, aMag, ref bdRef, bMag, (nuint)a.Length);
                    }
                    else throw new NotSupportedException($"Type {typeof(T).Name} is not supported.");
                }
                else if (typeof(T) == typeof(double))
                {
                    double aMag, bMag;
                    if (typeof(TResult) == typeof(float))
                    {
                        aMag = (float)(object)aMagnitude;
                        bMag = (float)(object)bMagnitude;
                    }
                    else if (typeof(TResult) == typeof(double))
                    {
                        aMag = (double)(object)aMagnitude;
                        bMag = (double)(object)bMagnitude;
                    }
                    else throw new NotSupportedException($"Type {typeof(T).Name} is not supported.");

                    ref var adRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<T, double>(a));
                    ref var bdRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<T, double>(b));

                    if (Sse2.IsSupported)
                    {
                        similarity = CosineSimilarityInternalX64(ref adRef, aMag, ref bdRef, bMag, (nuint)a.Length);
                    }
                    else if (AdvInstructionSet.Arm.IsSupported)
                    {
                        similarity = CosineSimilarityInternalNeon(ref adRef, aMag, ref bdRef, bMag, (nuint)a.Length);
                    }
                    else throw new NotSupportedException($"Type {typeof(T).Name} is not supported.");
                }
                else throw new NotSupportedException($"Type {typeof(T).Name} is not supported.");

                if (typeof(TResult) == typeof(float))
                {
                    return (TResult)(object)(float)similarity;
                }

                return (TResult)(object)similarity;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static TResult CosineSimilarityIntegersAvx512<TResult>(ReadOnlySpan<sbyte> a, TResult aMagnitude, ReadOnlySpan<sbyte> b, TResult bMagnitude)
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                if (Avx512BW.IsSupported == false || Avx512F.IsSupported == false)
                    throw new NotSupportedException("This method should not be called on the current architecture");

                // 1) 512-bit accumulators for ab, a2, b2
                var abVec = Vector512<int>.Zero;
                var a2Vec = Vector512<int>.Zero;
                var b2Vec = Vector512<int>.Zero;

                int i = a.Length;

                ref sbyte aRef = ref MemoryMarshal.GetReference(a);
                ref sbyte bRef = ref MemoryMarshal.GetReference(b);

            Loop:

                // 2) load full 32-byte block
                Vector512<short> aVec = Avx512BW.ConvertToVector512Int16(Vector256.LoadUnsafe(ref aRef));
                Vector512<short> bVec = Avx512BW.ConvertToVector512Int16(Vector256.LoadUnsafe(ref bRef));

                // 3) multiply-add pairs: i16×i16→i32
                abVec = Avx512F.Add(abVec, Avx512BW.MultiplyAddAdjacent(aVec, bVec));
                a2Vec = Avx512F.Add(a2Vec, Avx512BW.MultiplyAddAdjacent(aVec, aVec));
                b2Vec = Avx512F.Add(b2Vec, Avx512BW.MultiplyAddAdjacent(bVec, bVec));

                i -= Vector256<sbyte>.Count;
                aRef = ref Unsafe.Add(ref aRef, Vector256<sbyte>.Count);
                bRef = ref Unsafe.Add(ref bRef, Vector256<sbyte>.Count);

                if (i >= Vector256<sbyte>.Count)
                    goto Loop;

                // 4) horizontal reductions
                int ab = Vector512.Sum(abVec);
                int a2 = Vector512.Sum(a2Vec);
                int b2 = Vector512.Sum(b2Vec);

                // Tail loop for remaining elements
                while (i > 0)
                {
                    ab += aRef * bRef;
                    a2 += aRef * aRef;
                    b2 += bRef * bRef;

                    i--;
                    aRef = ref Unsafe.Add(ref aRef, 1);
                    bRef = ref Unsafe.Add(ref bRef, 1);
                }

                // Special cases
                if (a2 == 0 && b2 == 0)
                    return TResult.CreateTruncating(double.NaN); // Both zero vectors: nan
                if (ab == 0)
                    return TResult.Zero; // Orthogonal or one zero: distance = 1, similarity 0

                // Apply magnitudes and normalise
                TResult fab = TResult.CreateTruncating(ab) * aMagnitude * bMagnitude;
                TResult fa2 = TResult.CreateTruncating(a2) * aMagnitude * aMagnitude;
                TResult fb2 = TResult.CreateTruncating(b2) * bMagnitude * bMagnitude;

                return Vectorized256.CosineSimilarityNormalize<TResult, TResult>(fab, fa2, fb2);
            }
        }

        public static class Vectorized256
        {

            [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
            internal static TResult CosineSimilarityNormalizeSse<T, TResult>(T ab, T a2, T b2)
                where T : unmanaged, IRootFunctions<T>, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                // Create a 128-bit vector with a2 in the high lane and b2 in the low lane.
                // Note: _mm_set_pd(a2, b2) in C sets lane1=a2 and lane0=b2.
                // In .NET, Vector128.Create(x, y) sets lane0 = x and lane1 = y.
                // So we swap the order.
                var squares = Vector128.Create(double.CreateTruncating(b2), double.CreateTruncating(a2));

                // Compute approximate reciprocal square root (single precision).
                var rsqrts = Sse2.ConvertToVector128Double(
                    Sse.ReciprocalSqrt(
                        Sse2.ConvertToVector128Single(squares))
                );

                // Newton-Raphson iteration for reciprocal square root:
                // https://en.wikipedia.org/wiki/Newton%27s_method
                rsqrts = Sse2.Add(
                    Sse2.Multiply(Vector128.Create(1.5d), rsqrts),
                    Sse2.Multiply(
                        Sse2.Multiply(
                            Sse2.Multiply(squares, Vector128.Create(-0.5d)),
                            rsqrts),
                        Sse2.Multiply(rsqrts, rsqrts)
                    )
                );

                // Extract the results.
                // According to our lane ordering:
                //   - Lane 0 contains b2 reciprocal.
                //   - Lane 1 contains a2 reciprocal.
                double b2Reciprocal = rsqrts.ToScalar(); // lane 0
                double a2Reciprocal = Sse2.UnpackHigh(rsqrts, rsqrts).ToScalar(); // lane 1

                return TResult.CreateTruncating(double.CreateTruncating(ab) * a2Reciprocal * b2Reciprocal);
            }

            [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
            internal static TResult CosineSimilarityNormalizeNeon<T, TResult>(T ab, T a2, T b2)
                where T : unmanaged, IRootFunctions<T>, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                // Create vector with the squared magnitudes
                var squares = Vector64.Create(float.CreateTruncating(b2), float.CreateTruncating(a2));

                // Compute reciprocal square root approximation
                Vector64<float> rsqrts = AdvSimd.ReciprocalSquareRootEstimate(squares);

                // Perform two rounds of Newton-Raphson refinement for better accuracy
                // Formula: rsqrt_new = rsqrt * (1.5 - 0.5 * x * rsqrt^2)
                // Which can be rewritten as: rsqrt * vrsqrts(x * rsqrt, rsqrt)
                rsqrts = AdvSimd.Multiply(rsqrts, AdvSimd.ReciprocalSquareRootStep(AdvSimd.Multiply(squares, rsqrts), rsqrts));
                rsqrts = AdvSimd.Multiply(rsqrts, AdvSimd.ReciprocalSquareRootStep(AdvSimd.Multiply(squares, rsqrts), rsqrts));

                // Extract the refined reciprocal square roots
                float a2Reciprocal = rsqrts.GetElement(0);
                float b2Reciprocal = rsqrts.GetElement(1);

                return TResult.CreateTruncating(float.CreateTruncating(ab) * a2Reciprocal * b2Reciprocal);
            }

            [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
            internal static TResult CosineSimilarityNormalize<T, TResult>(T ab, T a2, T b2)
                where T : unmanaged, IRootFunctions<T>, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                if (Sse2.IsSupported)
                    return CosineSimilarityNormalizeSse<T, TResult>(ab, a2, b2);

                if (AdvInstructionSet.Arm.IsSupported)
                    return CosineSimilarityNormalizeNeon<T, TResult>(ab, a2, b2);

                // Fallback to the serial implementation if SIMD is not supported
                return Serial.CosineSimilarityNormalize<T, TResult>(ab, a2, b2);
            }


            [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
            public static TResult CosineSimilarity<T, TResult>(ReadOnlySpan<T> a, ReadOnlySpan<T> b)
                where T : unmanaged, IRootFunctions<T>, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                Vector256<T> abVec = Vector256<T>.Zero;
                Vector256<T> a2Vec = Vector256<T>.Zero;
                Vector256<T> b2Vec = Vector256<T>.Zero;

                int i = a.Length;
                ref T aRef = ref MemoryMarshal.GetReference(a);
                ref T bRef = ref MemoryMarshal.GetReference(b);

                Loop:
                // PERF: The reason why this would work on hardware not supporting 512-bit vectors is
                // that it will effectively create 2 lanes (xmm and ymm) of 256-bit vectors. And because
                // there are no overlapping lanes, there will be less pipeline dependencies hiding latency
                // of the instructions themselves.
                Vector256<T> aVec = Vector256.LoadUnsafe(ref aRef);
                Vector256<T> bVec = Vector256.LoadUnsafe(ref bRef);

                abVec = Arithmetics.MultiplyAddEstimate(aVec, bVec, abVec);
                a2Vec = Arithmetics.MultiplyAddEstimate(aVec, aVec, a2Vec);
                b2Vec = Arithmetics.MultiplyAddEstimate(bVec, bVec, b2Vec);

                i -= Vector256<T>.Count;
                aRef = ref Unsafe.Add(ref aRef, Vector256<T>.Count);
                bRef = ref Unsafe.Add(ref bRef, Vector256<T>.Count);
                if (i >= Vector256<T>.Count)
                    goto Loop;

                T ab = Vector256.Sum(abVec);
                T a2 = Vector256.Sum(a2Vec);
                T b2 = Vector256.Sum(b2Vec);
                while (i > 0)
                {
                    ab += aRef * bRef;
                    a2 += aRef * aRef;
                    b2 += bRef * bRef;

                    i--;
                    aRef = ref Unsafe.Add(ref aRef, 1);
                    bRef = ref Unsafe.Add(ref bRef, 1);
                }

                // Special cases
                if (T.IsZero(a2) && T.IsZero(b2))
                    return TResult.CreateTruncating(double.NaN); // Both zero vectors: nan
                if (T.IsZero(ab))
                    return TResult.Zero; // Orthogonal or one zero: distance = 1, similarity 0

                // Normalization
                return CosineSimilarityNormalize<T, TResult>(ab, a2, b2);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static TResult CosineSimilarityIntegersAvx2<TResult>(ReadOnlySpan<sbyte> a, TResult aMagnitude, ReadOnlySpan<sbyte> b, TResult bMagnitude)
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                if (Avx2.IsSupported == false)
                    throw new NotSupportedException("This method should not be called on the current architecture");

                //  Prepare six 256-bit accumulators: low/high lanes for ab, a2, b2
                Vector256<int> abLo = Vector256<int>.Zero, abHi = Vector256<int>.Zero;
                Vector256<int> a2Lo = Vector256<int>.Zero, a2Hi = Vector256<int>.Zero;
                Vector256<int> b2Lo = Vector256<int>.Zero, b2Hi = Vector256<int>.Zero;

                int i = a.Length;

                ref sbyte aRef = ref MemoryMarshal.GetReference(a);
                ref sbyte bRef = ref MemoryMarshal.GetReference(b);

                Loop:
                // Load 32 signed-bytes from each span
                Vector256<sbyte> aVec = Vector256.LoadUnsafe(ref aRef);
                Vector256<sbyte> bVec = Vector256.LoadUnsafe(ref bRef);

                // Unpack signed‐byte → signed‐short for low/high 128‐bit lanes
                Vector256<short> aVecLo = Avx2.ConvertToVector256Int16(aVec.GetLower());
                Vector256<short> aVecHi = Avx2.ConvertToVector256Int16(aVec.GetUpper());
                Vector256<short> bVecLo = Avx2.ConvertToVector256Int16(bVec.GetLower());
                Vector256<short> bVecHi = Avx2.ConvertToVector256Int16(bVec.GetUpper());

                // Multiply‐add adjacent pairs: produces four i32 results per 128‐bit lane

                // Perform i16×i16→i32 multiply-add on adjacent pairs:
                //    - ab += a * b
                //    - a2 += a * a
                //    - b2 += b * b
                abLo = Avx2.Add(abLo, Avx2.MultiplyAddAdjacent(aVecLo, bVecLo));
                abHi = Avx2.Add(abHi, Avx2.MultiplyAddAdjacent(aVecHi, bVecHi));

                a2Lo = Avx2.Add(a2Lo, Avx2.MultiplyAddAdjacent(aVecLo, aVecLo));
                a2Hi = Avx2.Add(a2Hi, Avx2.MultiplyAddAdjacent(aVecHi, aVecHi));

                b2Lo = Avx2.Add(b2Lo, Avx2.MultiplyAddAdjacent(bVecLo, bVecLo));
                b2Hi = Avx2.Add(b2Hi, Avx2.MultiplyAddAdjacent(bVecHi, bVecHi));

                i -= Vector256<sbyte>.Count;
                aRef = ref Unsafe.Add(ref aRef, Vector256<sbyte>.Count);
                bRef = ref Unsafe.Add(ref bRef, Vector256<sbyte>.Count);
                if (i >= Vector256<sbyte>.Count)
                    goto Loop;

                // Horizontal reduction across the two 128‐bit halves
                int ab = Vector256.Sum(Avx2.Add(abLo, abHi));
                int a2 = Vector256.Sum(Avx2.Add(a2Lo, a2Hi));
                int b2 = Vector256.Sum(Avx2.Add(b2Lo, b2Hi));

                // Tail loop for remaining elements
                while (i > 0)
                {
                    ab += aRef * bRef;
                    a2 += aRef * aRef;
                    b2 += bRef * bRef;

                    i--;
                    aRef = ref Unsafe.Add(ref aRef, 1);
                    bRef = ref Unsafe.Add(ref bRef, 1);
                }

                // Special cases
                if (a2 == 0 && b2 == 0)
                    return TResult.CreateTruncating(double.NaN); // Both zero vectors: nan
                if (ab == 0)
                    return TResult.Zero; // Orthogonal or one zero: distance = 1, similarity 0

                // Apply magnitudes and normalise
                TResult fab = TResult.CreateTruncating(ab) * aMagnitude * bMagnitude;
                TResult fa2 = TResult.CreateTruncating(a2) * aMagnitude * aMagnitude;
                TResult fb2 = TResult.CreateTruncating(b2) * bMagnitude * bMagnitude;
                return CosineSimilarityNormalize<TResult, TResult>(fab, fa2, fb2);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static TResult CosineSimilarityIntegersNeon<TResult>(ReadOnlySpan<sbyte> a, TResult aMagnitude, ReadOnlySpan<sbyte> b, TResult bMagnitude)
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                if (Dp.IsSupported == false)
                    throw new NotSupportedException("This method should not be called on the current architecture");

                Vector128<int> abVec = Vector128<int>.Zero;
                Vector128<int> a2Vec = Vector128<int>.Zero;
                Vector128<int> b2Vec = Vector128<int>.Zero;

                int i = a.Length;

                ref sbyte aRef = ref MemoryMarshal.GetReference(a);
                ref sbyte bRef = ref MemoryMarshal.GetReference(b);
                
                Loop:
                
                // Load 16 signed-bytes from each span
                Vector128<sbyte> aVec = Vector128.LoadUnsafe(ref aRef);
                Vector128<sbyte> bVec = Vector128.LoadUnsafe(ref bRef);

                // 4 × dot-product accumulating per 4 lanes
                abVec = Dp.DotProduct(abVec, aVec, bVec);
                a2Vec = Dp.DotProduct(a2Vec, aVec, aVec);
                b2Vec = Dp.DotProduct(b2Vec, bVec, bVec);

                i -= Vector128<sbyte>.Count;
                aRef = ref Unsafe.Add(ref aRef, Vector128<sbyte>.Count);
                bRef = ref Unsafe.Add(ref bRef, Vector128<sbyte>.Count);
                if (i >= Vector128<sbyte>.Count)
                    goto Loop;

                int ab = Vector128.Sum(abVec);
                int a2 = Vector128.Sum(a2Vec);
                int b2 = Vector128.Sum(b2Vec);
                while (i > 0)
                {
                    ab += aRef * bRef;
                    a2 += aRef * aRef;
                    b2 += bRef * bRef;

                    i--;
                    aRef = ref Unsafe.Add(ref aRef, 1);
                    bRef = ref Unsafe.Add(ref bRef, 1);
                }

                // Special cases
                if (a2 == 0 && b2 == 0)
                    return TResult.CreateTruncating(double.NaN); // Both zero vectors: nan
                if (ab == 0)
                    return TResult.Zero; // Orthogonal or one zero: distance = 1, similarity 0

                // Apply magnitudes and normalise
                TResult fab = TResult.CreateTruncating(ab) * aMagnitude * bMagnitude;
                TResult fa2 = TResult.CreateTruncating(a2) * aMagnitude * aMagnitude;
                TResult fb2 = TResult.CreateTruncating(b2) * bMagnitude * bMagnitude;
                return CosineSimilarityNormalize<TResult, TResult>(fab, fa2, fb2);
            }
        }

        /// <summary>Computes the bitwise Hamming distance between two equal-length tensors of values.</summary>
        /// <param name="x">The first tensor, represented as a span.</param>
        /// <param name="y">The second tensor, represented as a span.</param>
        /// <returns>The number of bits that differ between the two spans.</returns>
        /// <exception cref="ArgumentException">Length of <paramref name="x" /> must be same as length of <paramref name="y" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="x" /> and <paramref name="y" /> must not be empty.</exception>
        public static long HammingBitDistance<T>(ReadOnlySpan<T> x, ReadOnlySpan<T> y) where T : unmanaged, IBinaryInteger<T>
        {
            var xAsByte = MemoryMarshal.Cast<T, byte>(x);
            var yAsByte = MemoryMarshal.Cast<T, byte>(y);

            ref byte xRef = ref MemoryMarshal.GetReference(xAsByte);
            ref byte yRef = ref MemoryMarshal.GetReference(yAsByte);

            const int Word = sizeof(ulong);
            const int N = Word * 3;     

            ulong count1 = 0;
            ulong count2 = 0;
            ulong count3 = 0;

            int offset = 0;
            int length = xAsByte.Length;

            // Process all full 24-byte blocks
            while (offset + N <= length)
            {
                int byteOffset = offset;
                ulong xBits1 = Unsafe.ReadUnaligned<ulong>(ref Unsafe.AddByteOffset(ref xRef, byteOffset));
                ulong yBits1 = Unsafe.ReadUnaligned<ulong>(ref Unsafe.AddByteOffset(ref yRef, byteOffset));
                count1 += ulong.PopCount(xBits1 ^ yBits1);

                byteOffset = (offset + Word);
                ulong xBits2 = Unsafe.ReadUnaligned<ulong>(ref Unsafe.AddByteOffset(ref xRef, byteOffset));
                ulong yBits2 = Unsafe.ReadUnaligned<ulong>(ref Unsafe.AddByteOffset(ref yRef, byteOffset));
                count2 += ulong.PopCount(xBits2 ^ yBits2);

                byteOffset = (offset + 2 * Word);
                ulong xBits3 = Unsafe.ReadUnaligned<ulong>(ref Unsafe.AddByteOffset(ref xRef, byteOffset));
                ulong yBits3 = Unsafe.ReadUnaligned<ulong>(ref Unsafe.AddByteOffset(ref yRef, byteOffset));
                count3 += ulong.PopCount(xBits3 ^ yBits3);

                offset += N;
            }

            ulong count = count1 + count2 + count3;

            for (int i = offset; i < length; i++)
            {
                var xor = Unsafe.Add(ref xRef, i) ^ Unsafe.Add(ref yRef, i);
                count += (ulong)int.PopCount(xor);
            }

            return (long)count;
        }
    }
}
