using System;
using System.Numerics.Tensors;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.Arm;
using System.Runtime.Intrinsics.X86;
using FastTests.Voron.FixedSize;
using Sparrow;
using Sparrow.Server.Tensors;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public unsafe class TensorsTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        private const float Eps = 1e-6f;

        // Test that for identical vectors, we get maximum similarity (and so a distance of zero).
        [RavenFact(RavenTestCategory.Core)]
        public void IdenticalVectors_ReturnsMaxSimilarity_ZeroDistance()
        {
            float[] vector = [1f, 2f, 3f, 4f];
            var span = new ReadOnlySpan<float>(vector);

            float similarity = Functions.CosineSimilarity(span, span);
            float distance = Functions.CosineDistance(span, span);

            float expectedSim = TensorPrimitives.CosineSimilarity(span, span);
            float expectedDistance = 1.0f - expectedSim;

            // Expected behavior: identical vectors yield similarity==1.0 so distance==0.0.
            // (If your implementation were really computing similarity, this is what you’d expect.)
            Assert.InRange(similarity, (float)(expectedSim - Eps), (float)(expectedSim + Eps));
            Assert.InRange(distance, (float)(expectedDistance - Eps), (float)(expectedDistance + Eps));
        }


        // Test that for identical vectors, we get maximum similarity (and so a distance of zero).
        [RavenFact(RavenTestCategory.Core)]
        public void IdenticalVectors_ReturnsMaxHammingSimilarity_ZeroDistance()
        {
            byte[] vector = [218, 0, 55, 87, 97, 77, 10, 66, 255, 47];
            var span = new ReadOnlySpan<byte>(vector);

            float distance = Functions.HammingBitDistance(span, span);
            float expectedDistance = TensorPrimitives.HammingBitDistance(span, span);

            Assert.Equal(expectedDistance, distance);
        }

        // Test for orthogonal vectors.
        // Conventionally, for orthogonal vectors, cosine similarity should be 0 and so distance should be 1.
        [RavenTheory(RavenTestCategory.Core)]
        [InlineData(2)]
        [InlineData(8)]
        [InlineData(16)]
        [InlineData(17)]
        [InlineData(32)]
        [InlineData(256)]
        [InlineData(1000)]
        public void OrthogonalVectors_ShouldYieldLowSimilarity_HighDistance(int size)
        {
            float[] a = new float[size];
            float[] b = new float[size];

            a[size-1] = 1f;
            b[0] = 1f;

            var similarity = Functions.CosineSimilarity<float>(a, b);
            var distance = Functions.CosineDistance<float>(a, b);

            float expectedSim = TensorPrimitives.CosineSimilarity(a, b);
            float expectedDistance = 1.0f - expectedSim;

            // Expected behavior: orthogonal vectors yield similarity==0.0
            // (If your implementation were really computing similarity, this is what you’d expect.)
            Assert.InRange(similarity, (float)(expectedSim - Eps), (float)(expectedSim + Eps));
            Assert.InRange(distance, (float)(expectedDistance - Eps), (float)(expectedDistance + Eps));
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed]
        public void Ensure_NoBufferOverflow_Float(int seed)
        {
            var generator = new Random(seed);

            float[] a = new float[1024];
            float[] b = new float[1024];

            for (int i = 0; i < 32; i++)
            {
                // We are testing vectorized versions, which won't be called if we call the public versions. 
                var size = generator.Next(Vector512<float>.Count, 512);
                for (int j = 0; j < a.Length; j++)
                {
                    a[j] = 0;
                    b[j] = 0;
                }

                var location = generator.Next(128) + 1;

                a[location - 1] = 0.5f;
                b[location - 1] = 1f;
                a[location + size + 1] = 0.5f;
                b[location + size + 1] = 1f;

                void RunFunc(delegate* managed<ReadOnlySpan<float>, ReadOnlySpan<float>, float> func)
                {
                    Span<float> aSpan = a.AsSpan().Slice(location, size);
                    Span<float> bSpan = b.AsSpan().Slice(location,  size);
                    Assert.Equal(func(aSpan, bSpan), float.NaN);

                    aSpan = a.AsSpan().Slice(location - 1, size + 1);
                    bSpan = b.AsSpan().Slice(location - 1, size + 1);
                    Assert.NotEqual(func(aSpan, bSpan), float.NaN);
                }

                void RunWithMagnitudeFunc(delegate* managed<ReadOnlySpan<float>, float, ReadOnlySpan<float>, float, float> func)
                {
                    Span<float> aSpan = a.AsSpan().Slice(location,size);
                    Span<float> bSpan = b.AsSpan().Slice(location, size);
                    Assert.Equal(func(aSpan, 1.0f, bSpan, 1.0f), float.NaN);

                    // Verify they match within a small tolerance.
                    aSpan = a.AsSpan().Slice(location - 1, size + 1);
                    bSpan = b.AsSpan().Slice(location - 1, size + 1);
                    Assert.NotEqual(func(aSpan, 1.0f, bSpan, 1.0f), float.NaN);
                }

                // These are the normal functions.
                RunFunc(&Functions.CosineSimilarity);
                RunFunc(&Functions.CosineDistance);
                RunWithMagnitudeFunc(&Functions.CosineSimilarity);
                RunWithMagnitudeFunc(&Functions.CosineDistance);
                RunWithMagnitudeFunc(&Functions.Vectorized512.CosineSimilarity);
            }
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed]
        public void Ensure_NoBufferOverflow_Double(int seed)
        {
            var generator = new Random(seed);

            double[] a = new double[1024];
            double[] b = new double[1024];

            for (int i = 0; i < 32; i++)
            {
                // We are testing vectorized versions, which won't be called if we call the public versions. 
                var size = generator.Next(Vector512<double>.Count, 512);
                for (int j = 0; j < a.Length; j++)
                {
                    a[j] = 0;
                    b[j] = 0;
                }

                var location = generator.Next(128) + 1;

                a[location - 1] = 0.5f;
                b[location - 1] = 1f;
                a[location + size + 1] = 0.5f;
                b[location + size + 1] = 1f;

                void RunFunc(delegate* managed<ReadOnlySpan<double>, ReadOnlySpan<double>, double> func)
                {
                    Span<double> aSpan = a.AsSpan().Slice(location, size);
                    Span<double> bSpan = b.AsSpan().Slice(location, size);
                    Assert.Equal(func(aSpan, bSpan), float.NaN);

                    aSpan = a.AsSpan().Slice(location - 1, size + 1);
                    bSpan = b.AsSpan().Slice(location - 1, size + 1);
                    Assert.NotEqual(func(aSpan, bSpan), float.NaN);
                }

                void RunWithMagnitudeFunc(delegate* managed<ReadOnlySpan<double>, float, ReadOnlySpan<double>, float, float> func)
                {
                    Span<double> aSpan = a.AsSpan().Slice(location, size);
                    Span<double> bSpan = b.AsSpan().Slice(location, size);
                    Assert.Equal(func(aSpan, 1.0f, bSpan, 1.0f), float.NaN);

                    // Verify they match within a small tolerance.
                    aSpan = a.AsSpan().Slice(location - 1, size + 1);
                    bSpan = b.AsSpan().Slice(location - 1, size + 1);
                    Assert.NotEqual(func(aSpan, 1.0f, bSpan, 1.0f), float.NaN);
                }

                // These are the normal functions.
                RunFunc(&Functions.CosineSimilarity);
                RunFunc(&Functions.CosineDistance);
                RunWithMagnitudeFunc(&Functions.CosineSimilarity);
                RunWithMagnitudeFunc(&Functions.CosineDistance);
                RunWithMagnitudeFunc(&Functions.Vectorized512.CosineSimilarity);
            }
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed]
        public void Ensure_NoBufferOverflow_Integers(int seed)
        {
            var generator = new Random(seed);

            for (int i = 0; i < 32; i++)
            {
                sbyte[] a = new sbyte[1024];
                sbyte[] b = new sbyte[1024];

                // We are testing vectorized versions, which won't be called if we call the public versions. 
                var size = generator.Next(Vector512<sbyte>.Count, 512);
                for (int j = 0; j < a.Length; j++)
                {
                    a[j] = 0;
                    b[j] = 0;
                }

                var location = generator.Next(128) + 1;

                a[location - 1] = 55;
                b[location - 1] = 45;
                a[location + size + 1] = 55;
                b[location + size + 1] = 45;

                void RunFunc(delegate* managed<ReadOnlySpan<sbyte>, float, ReadOnlySpan<sbyte>, float, float> func)
                {
                    Span<sbyte> aSpan = a.AsSpan().Slice(location, size);
                    Span<sbyte> bSpan = b.AsSpan().Slice(location, size);
                    Assert.Equal(func(aSpan, 1.0f, bSpan, 1.0f), float.NaN);

                    aSpan = a.AsSpan().Slice(location - 1, size + 1);
                    bSpan = b.AsSpan().Slice(location - 1, size + 1);
                    Assert.NotEqual(func(aSpan, 1.0f, bSpan, 1.0f), float.NaN);
                }

                void RunWithMagnitudeFunc(delegate* managed<ReadOnlySpan<sbyte>, float, ReadOnlySpan<sbyte>, float, float> func)
                {
                    Span<sbyte> aSpan = a.AsSpan().Slice(location, size);
                    Span<sbyte> bSpan = b.AsSpan().Slice(location, size);
                    Assert.Equal(func(aSpan, 1.0f, bSpan, 1.0f), float.NaN);

                    // Verify they match within a small tolerance.
                    aSpan = a.AsSpan().Slice(location - 1, size + 1);
                    bSpan = b.AsSpan().Slice(location - 1, size + 1);
                    Assert.NotEqual(func(aSpan, 1.0f, bSpan, 1.0f), float.NaN);
                }


                // These are the normal functions.
                RunFunc(&Functions.CosineSimilarity);
                RunFunc(&Functions.CosineDistance);
                RunWithMagnitudeFunc(&Functions.CosineSimilarity);
                RunWithMagnitudeFunc(&Functions.CosineDistance);

                // AVX-2 path
                if (AdvInstructionSet.X86.IsSupportedAvx256 && size >= Vector256<sbyte>.Count)
                    RunWithMagnitudeFunc(&Functions.Vectorized256.CosineSimilarityIntegersAvx2);

                if (AdvInstructionSet.Arm.IsSupported && Dp.IsSupported && size >= Vector128<sbyte>.Count)
                    RunWithMagnitudeFunc(&Functions.Vectorized128.CosineSimilarityIntegersNeon);

                // AVX-512 path
                if (Avx512BW.IsSupported && Avx512F.IsSupported && size >= Vector512<sbyte>.Count)
                    RunWithMagnitudeFunc(&Functions.Vectorized512.CosineSimilarityIntegersAvx512);
            }
        }

        // Test for vectors that are both zero.
        // Many definitions choose to define the similarity of two zero vectors as 1 (so that distance is 0).
        [RavenTheory(RavenTestCategory.Core)]
        [InlineData(2)]
        [InlineData(8)]
        [InlineData(16)]
        [InlineData(17)]
        [InlineData(32)]
        [InlineData(33)]
        [InlineData(256)]
        [InlineData(1000)]
        public void BothZeroVectors_ProducesDefinedSimilarityAndDistance(int size)
        {
            float[] a = new float[size];
            float[] b = new float[size];

            var similarity = Functions.CosineSimilarity<float>(a, b);
            var distance = Functions.CosineDistance<float>(a, b);

            // Compute reference similarity (using conventional cosine similarity).
            float expectedSim = TensorPrimitives.CosineSimilarity(a, b);
            float expectedDistance = 1.0f - expectedSim;

            // Verify they match within a small tolerance.

            Assert.InRange(similarity, (float)(expectedSim - Eps), (float)(expectedSim + Eps));
            Assert.InRange(distance, (float)(expectedDistance - Eps), (float)(expectedDistance + Eps));
        }

        // A randomized test to compare your implementation with a reference implementation.
        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed]
        public void RandomVectors_ReferenceComparison(int seed = 1337)
        {
            var rnd = new Random(seed);
            int size = rnd.Next(1024) + 1;

            // Generate two random vectors of the same size.
            float[] vector1 = new float[size];
            float[] vector2 = new float[size];
            for (int i = 0; i < size; i++)
            {
                vector1[i] = (float)rnd.NextDouble();
                vector2[i] = (float)rnd.NextDouble();
            }

            // Compute reference similarity (using conventional cosine similarity).
            float expectedSim = TensorPrimitives.CosineSimilarity(vector1, vector2);
            float expectedDistance = 1.0f - expectedSim;

            void RunFunc(delegate* managed<ReadOnlySpan<float>, ReadOnlySpan<float>, float> func, float expected)
            {
                float actual = func(vector1.AsSpan(), vector2.AsSpan());

                // Verify they match within a small tolerance.
                Assert.InRange(actual, (float)(expected - Eps), (float)(expected + Eps));
            }

            void RunWithMagnitudeFunc(delegate* managed<ReadOnlySpan<float>, float, ReadOnlySpan<float>, float, float> func, float expected)
            {
                float actual = func(vector1.AsSpan(), 1.0f, vector2.AsSpan(), 1.0f);

                // Verify they match within a small tolerance.
                Assert.InRange(actual, (float)(expected - Eps), (float)(expected + Eps));
            }

            // These are the normal functions.
            RunFunc(&Functions.CosineSimilarity, expectedSim);
            RunFunc(&Functions.CosineDistance, expectedDistance);
            RunWithMagnitudeFunc(&Functions.CosineSimilarity, expectedSim);
            RunWithMagnitudeFunc(&Functions.CosineDistance, expectedDistance);
            RunWithMagnitudeFunc(&Functions.Vectorized512.CosineSimilarity, expectedSim);
        }

        // A randomized test to compare your implementation with a reference implementation.
        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed]
        public void RandomVectors_ReferenceComparison_Double(int seed = 1337)
        {
            var rnd = new Random(seed);
            int size = rnd.Next(1024) + 1;

            // Generate two random vectors of the same size.
            double[] vector1 = new double[size];
            float[] fvector1 = new float[size];
            double[] vector2 = new double[size];
            float[] fvector2 = new float[size];
            for (int i = 0; i < size; i++)
            {
                vector1[i] = rnd.NextDouble();
                fvector1[i] = (float)vector1[i];
                vector2[i] = rnd.NextDouble();
                fvector2[i] = (float)vector2[i];
            }

            // Compute reference similarity (using conventional cosine similarity).
            float expectedSim = TensorPrimitives.CosineSimilarity(fvector1, fvector2);
            float expectedDistance = 1.0f - expectedSim;

            void RunFunc(delegate* managed<ReadOnlySpan<double>, ReadOnlySpan<double>, double> func, float expected)
            {
                double actual = func(vector1.AsSpan(), vector2.AsSpan());

                // Verify they match within a small tolerance.
                Assert.InRange(actual, (float)(expected - Eps), (float)(expected + Eps));
            }

            void RunWithMagnitudeFunc(delegate* managed<ReadOnlySpan<double>, float, ReadOnlySpan<double>, float, float> func, float expected)
            {
                float actual = func(vector1.AsSpan(), 1.0f, vector2.AsSpan(), 1.0f);

                // Verify they match within a small tolerance.
                Assert.InRange(actual, (float)(expected - Eps), (float)(expected + Eps));
            }

            // These are the normal functions.
            RunFunc(&Functions.CosineSimilarity, expectedSim);
            RunFunc(&Functions.CosineDistance, expectedDistance);
            RunWithMagnitudeFunc(&Functions.CosineSimilarity, expectedSim);
            RunWithMagnitudeFunc(&Functions.CosineDistance, expectedDistance);
            RunWithMagnitudeFunc(&Functions.Vectorized512.CosineSimilarity, expectedSim);
        }

        // A randomized test to compare your implementation with a reference implementation.
        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed]
        [InlineData(531226929)]
        public void RandomVectors_ReferenceComparison_Integers(int seed = 1337)
        {
            var rnd = new Random(seed);
            int size = rnd.Next(1024) + 1;

            // Generate two random vectors of the same size.
            sbyte[] vector1 = new sbyte[size];
            sbyte[] vector2 = new sbyte[size];
            float[] fvector1 = new float[size];
            float[] fvector2 = new float[size];
            for (int i = 0; i < size; i++)
            {
                vector1[i] = (sbyte)rnd.Next();
                fvector1[i] = vector1[i];
                vector2[i] = (sbyte)rnd.Next();
                fvector2[i] = vector2[i];
            }

            // Compute reference similarity (using conventional cosine similarity).
            float expectedSim = TensorPrimitives.CosineSimilarity(fvector1, fvector2);
            float expectedDistance = 1.0f - expectedSim;

            void RunWithMagnitudeFunc(delegate* managed<ReadOnlySpan<sbyte>, float, ReadOnlySpan<sbyte>, float, float> func, float expected)
            {
                float actual = func(vector1.AsSpan(), 1.0f, vector2.AsSpan(), 1.0f);

                // Verify they match within a small tolerance.
                Assert.InRange(actual, (float)(expected - Eps), (float)(expected + Eps));
            }

            // These are the normal functions.
            RunWithMagnitudeFunc(&Functions.CosineSimilarity, expectedSim);
            RunWithMagnitudeFunc(&Functions.CosineDistance, expectedDistance);

            // AVX-2 path
            if (AdvInstructionSet.X86.IsSupportedAvx256 && size >= Vector256<sbyte>.Count)
                RunWithMagnitudeFunc(&Functions.Vectorized256.CosineSimilarityIntegersAvx2, expectedSim);

            if (AdvInstructionSet.Arm.IsSupported && Dp.IsSupported && size >= Vector128<sbyte>.Count)
                RunWithMagnitudeFunc(&Functions.Vectorized128.CosineSimilarityIntegersNeon, expectedSim);

            // AVX-512 path
            if (Avx512BW.IsSupported && Avx512F.IsSupported && size >= Vector512<sbyte>.Count)
                RunWithMagnitudeFunc(&Functions.Vectorized512.CosineSimilarityIntegersAvx512, expectedSim);
        }

        // A randomized test to compare your implementation with a reference implementation.
        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed]
        public void RandomVectors_ReferenceComparisonForQuantization(int seed)
        {
            var rnd = new Random(1337);
            int size = rnd.Next(1024) + 1;
            float aMagnitude = rnd.NextSingle();
            float bMagnitude = rnd.NextSingle();

            // Generate two random vectors of the same size.
            float[] fvector1 = new float[size];
            sbyte[] bvector1 = new sbyte[size];
            float[] fvector2 = new float[size];
            sbyte[] bvector2 = new sbyte[size];
            
            for (int i = 0; i < size; i++)
            {
                sbyte v1 = (sbyte)rnd.Next(sbyte.MinValue, sbyte.MaxValue);
                sbyte v2 = (sbyte)rnd.Next(sbyte.MinValue, sbyte.MaxValue);

                bvector1[i] = v1;
                bvector2[i] = v2;

                fvector1[i] = (float)v1 * aMagnitude;
                fvector2[i] = (float)v2 * bMagnitude;
            }

            // Compute reference similarity (using conventional cosine similarity).
            float expectedSim = Functions.Serial.CosineSimilarity<float, float>(fvector1, fvector2);

            void RunSimilarityTest(delegate* managed<ReadOnlySpan<sbyte>, float, ReadOnlySpan<sbyte>, float, float> func)
            {
                float sim = func(bvector1.AsSpan(), aMagnitude, bvector2.AsSpan(), bMagnitude);

                // Verify they match within a small tolerance.
                Assert.InRange(sim, (float)(expectedSim - Eps), (float)(expectedSim + Eps));
            }

            // Serial fallback via our non-generic shim
            RunSimilarityTest(&Functions.Serial.CosineSimilarity);

            // AVX-512 path
            if (Avx512BW.IsSupported && Avx512F.IsSupported && size >= Vector512<sbyte>.Count)
                RunSimilarityTest(&Functions.Vectorized512.CosineSimilarityIntegersAvx512);

            // AVX-2 path
            if (AdvInstructionSet.X86.IsSupportedAvx256 && size >= Vector256<sbyte>.Count)
                RunSimilarityTest(&Functions.Vectorized256.CosineSimilarityIntegersAvx2);

            // NEON path
            if (AdvInstructionSet.Arm.IsSupported && Dp.IsSupported && size >= Vector128<sbyte>.Count)
                RunSimilarityTest(&Functions.Vectorized128.CosineSimilarityIntegersNeon);
        }

        // A randomized test to compare your implementation with a reference implementation.
        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed]
        public void RandomVectors_HammingBitReferenceComparison(int seed)
        {
            var rnd = new Random(seed);
            int size = rnd.Next(1024) + 1;

            // Generate two random vectors of the same size.
            Span<byte> vector1 = new byte[size];
            Span<byte> vector2 = new byte[size];
            for (int i = 0; i < size; i++)
            {
                vector1[i] = (byte)rnd.Next(byte.MaxValue);
                vector2[i] = (byte)rnd.Next(byte.MaxValue);
            }

            // Compute reference similarity (using conventional cosine similarity).
            long expectedDistance = TensorPrimitives.HammingBitDistance<byte>(vector1, vector2);
            long actualDistance = Functions.HammingBitDistance<byte>(vector1, vector2);
            Assert.Equal(expectedDistance, actualDistance);
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed]
        public void Ensure_NoBufferOverflow_HammingBit(int seed)
        {
            var generator = new Random(seed);

            for (int i = 0; i < 32; i++)
            {
                byte[] a = new byte[1024];
                byte[] b = new byte[1024];

                // We are testing vectorized versions, which won't be called if we call the public versions. 
                var size = generator.Next(Vector512<sbyte>.Count, 512);
                for (int j = 0; j < a.Length; j++)
                {
                    a[j] = 0;
                    b[j] = 0;
                }

                var location = generator.Next(128) + 1;

                a[location - 1] = 55;
                b[location - 1] = 45;
                a[location + size + 1] = 55;
                b[location + size + 1] = 45;

                Span<byte> aSpan = a.AsSpan().Slice(location, size);
                Span<byte> bSpan = b.AsSpan().Slice(location, size);
                Assert.Equal(Functions.HammingBitDistance<byte>(aSpan, bSpan), 0);

                aSpan = a.AsSpan().Slice(location - 1, size + 1);
                bSpan = b.AsSpan().Slice(location - 1, size + 1);
                Assert.NotEqual(Functions.HammingBitDistance<byte>(aSpan, bSpan), 0);
            }
        }
    }
}
