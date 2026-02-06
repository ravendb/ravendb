using Sparrow.Compression;
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    /// <summary>
    /// Tests to verify backward compatibility between the optimized LZ4 implementation
    /// and the reference implementation. These tests ensure that:
    /// 1. Data compressed by the reference can be decompressed by optimized
    /// 2. Data compressed by optimized can be decompressed by reference
    /// 3. Both implementations produce valid output
    /// All tests use unmanaged memory with canary guard regions to detect buffer overruns.
    /// </summary>
    public unsafe class LZ4BackwardCompatibilityTests : NoDisposalNeeded
    {
        private const int GUARD = 64;
        private const int MAX_MISALIGN = 15;

        public LZ4BackwardCompatibilityTests(ITestOutputHelper output) : base(output)
        {
        }

        /// <summary>
        /// Test sizes covering critical boundaries in LZ4 algorithm
        /// </summary>
        public static IEnumerable<object[]> EdgeCaseSizes
        {
            get
            {
                return new[]
                {
                    // Very small sizes (below MFLIMIT)
                    new object[] { 1 },
                    new object[] { 4 },      // MINMATCH
                    new object[] { 8 },      // COPYLENGTH
                    new object[] { 12 },     // MFLIMIT boundary
                    new object[] { 13 },     // LZ4_minLength
                    new object[] { 14 },
                    new object[] { 15 },     // RUN_MASK
                    new object[] { 16 },

                    // Around common boundaries
                    new object[] { 63 },
                    new object[] { 64 },     // FASTLOOP_SAFE_DISTANCE (in newer versions)
                    new object[] { 65 },

                    // Around 128-byte threshold (BlittableWriter compression threshold)
                    new object[] { 127 },
                    new object[] { 128 },
                    new object[] { 129 },

                    // Around byte overflow
                    new object[] { 254 },
                    new object[] { 255 },
                    new object[] { 256 },
                    new object[] { 257 },

                    // Around 1KB
                    new object[] { 1023 },
                    new object[] { 1024 },
                    new object[] { 1025 },

                    // Around 4KB (page size)
                    new object[] { 4095 },
                    new object[] { 4096 },
                    new object[] { 4097 },

                    // Around 8KB (typical Voron page)
                    new object[] { 8191 },
                    new object[] { 8192 },
                    new object[] { 8193 },

                    // Around 64KB (ByU16 vs ByU32 boundary)
                    new object[] { 65534 },
                    new object[] { 65535 },
                    new object[] { 65536 },
                    new object[] { 65537 },

                    // Larger sizes
                    new object[] { 100000 },
                };
            }
        }

        public static IEnumerable<object[]> DataPatterns
        {
            get
            {
                return new[]
                {
                    new object[] { "zeros" },
                    new object[] { "ones" },
                    new object[] { "sequential" },
                    new object[] { "random_seeded" },
                    new object[] { "highly_compressible" },
                    new object[] { "incompressible" },
                };
            }
        }

        /// <summary>
        /// Allocates an unmanaged buffer with random-filled guard regions and random misalignment.
        /// Layout: [GUARD bytes] [misalign 0-15] [usable: size bytes] [GUARD bytes]
        /// </summary>
        private static byte* AllocGuarded(int size, Random rng, out byte* baseAlloc, out byte[] frontSnap, out byte[] backSnap)
        {
            int misalign = rng.Next(0, MAX_MISALIGN + 1);
            int total = GUARD + MAX_MISALIGN + size + GUARD;
            baseAlloc = (byte*)NativeMemory.Alloc((nuint)total);

            // Fill entire buffer with known random sequence (serves as canary AND garbage)
            var fill = new byte[total];
            rng.NextBytes(fill);
            fixed (byte* fillPtr = fill)
            {
                Buffer.MemoryCopy(fillPtr, baseAlloc, total, total);
            }

            byte* ptr = baseAlloc + GUARD + misalign;

            // Snapshot front guard: GUARD bytes before ptr
            frontSnap = new byte[GUARD];
            for (int i = 0; i < GUARD; i++)
                frontSnap[i] = *(ptr - GUARD + i);

            // Snapshot back guard: GUARD bytes after usable area
            backSnap = new byte[GUARD];
            for (int i = 0; i < GUARD; i++)
                backSnap[i] = *(ptr + size + i);

            return ptr;
        }

        /// <summary>
        /// Verifies that the guard regions around a guarded buffer are unchanged.
        /// </summary>
        private static void VerifyGuards(byte* ptr, int size, byte[] frontSnap, byte[] backSnap, string ctx)
        {
            // Check front guard
            for (int i = 0; i < GUARD; i++)
            {
                byte expected = frontSnap[i];
                byte actual = *(ptr - GUARD + i);
                Assert.True(expected == actual,
                    $"Front guard corrupted at offset {i} (relative to guard start) for {ctx}. Expected 0x{expected:X2}, got 0x{actual:X2}");
            }

            // Check back guard
            for (int i = 0; i < GUARD; i++)
            {
                byte expected = backSnap[i];
                byte actual = *(ptr + size + i);
                Assert.True(expected == actual,
                    $"Back guard corrupted at offset {i} (relative to usable end) for {ctx}. Expected 0x{expected:X2}, got 0x{actual:X2}");
            }
        }

        private static void FreeGuarded(byte* baseAlloc)
        {
            NativeMemory.Free(baseAlloc);
        }

        [RavenTheory(RavenTestCategory.Core | RavenTestCategory.Intrinsics)]
        [MemberData(nameof(EdgeCaseSizes))]
        public void ReferenceCompress_OptimizedDecompress(int size)
        {
            foreach (var pattern in new[] { "zeros", "sequential", "random_seeded", "highly_compressible" })
            {
                var input = GenerateTestData(size, pattern);
                var maxCompressedSize = LZ4Reference.MaximumOutputLength(size);
                var rng = new Random(42 + size + pattern.GetHashCode());

                byte* compBase, decompBase;
                byte[] compFront, compBack, decompFront, decompBack;

                var compressedPtr = AllocGuarded(maxCompressedSize, rng, out compBase, out compFront, out compBack);
                var decompressedPtr = AllocGuarded(size, rng, out decompBase, out decompFront, out decompBack);
                try
                {
                    fixed (byte* inputPtr = input)
                    {
                        // Compress with REFERENCE
                        int compressedSize = LZ4Reference.Encode64(inputPtr, compressedPtr, size, maxCompressedSize);
                        Assert.True(compressedSize > 0, $"Reference compression failed for size={size}, pattern={pattern}");

                        VerifyGuards(compressedPtr, maxCompressedSize, compFront, compBack, $"compressed buf after Reference.Encode64 size={size}, pattern={pattern}");

                        // Decompress with OPTIMIZED
                        int decompressedSize = LZ4.Decode64(compressedPtr, compressedSize, decompressedPtr, size, true);
                        Assert.Equal(size, decompressedSize);

                        VerifyGuards(decompressedPtr, size, decompFront, decompBack, $"decompressed buf after LZ4.Decode64 size={size}, pattern={pattern}");

                        // Verify data integrity
                        for (int i = 0; i < size; i++)
                        {
                            Assert.True(input[i] == decompressedPtr[i],
                                $"Data mismatch at position {i} for size={size}, pattern={pattern}. Expected {input[i]}, got {decompressedPtr[i]}");
                        }
                    }
                }
                finally
                {
                    FreeGuarded(compBase);
                    FreeGuarded(decompBase);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Core | RavenTestCategory.Intrinsics)]
        [MemberData(nameof(EdgeCaseSizes))]
        public void OptimizedCompress_ReferenceDecompress(int size)
        {
            foreach (var pattern in new[] { "zeros", "sequential", "random_seeded", "highly_compressible" })
            {
                var input = GenerateTestData(size, pattern);
                var maxCompressedSize = LZ4.MaximumOutputLength(size);
                var rng = new Random(42 + size + pattern.GetHashCode());

                byte* compBase, decompBase;
                byte[] compFront, compBack, decompFront, decompBack;

                var compressedPtr = AllocGuarded(maxCompressedSize, rng, out compBase, out compFront, out compBack);
                var decompressedPtr = AllocGuarded(size, rng, out decompBase, out decompFront, out decompBack);
                try
                {
                    fixed (byte* inputPtr = input)
                    {
                        // Compress with OPTIMIZED
                        int compressedSize = LZ4.Encode64(inputPtr, compressedPtr, size, maxCompressedSize);
                        Assert.True(compressedSize > 0, $"Optimized compression failed for size={size}, pattern={pattern}");

                        VerifyGuards(compressedPtr, maxCompressedSize, compFront, compBack, $"compressed buf after LZ4.Encode64 size={size}, pattern={pattern}");

                        // Decompress with REFERENCE
                        int decompressedSize = LZ4Reference.Decode64(compressedPtr, compressedSize, decompressedPtr, size, true);
                        Assert.Equal(size, decompressedSize);

                        VerifyGuards(decompressedPtr, size, decompFront, decompBack, $"decompressed buf after Reference.Decode64 size={size}, pattern={pattern}");

                        // Verify data integrity
                        for (int i = 0; i < size; i++)
                        {
                            Assert.True(input[i] == decompressedPtr[i],
                                $"Data mismatch at position {i} for size={size}, pattern={pattern}. Expected {input[i]}, got {decompressedPtr[i]}");
                        }
                    }
                }
                finally
                {
                    FreeGuarded(compBase);
                    FreeGuarded(decompBase);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Core | RavenTestCategory.Intrinsics)]
        [MemberData(nameof(EdgeCaseSizes))]
        public void OptimizedCompress_OptimizedDecompress(int size)
        {
            foreach (var pattern in new[] { "zeros", "sequential", "random_seeded", "highly_compressible" })
            {
                var input = GenerateTestData(size, pattern);
                var maxCompressedSize = LZ4.MaximumOutputLength(size);
                var rng = new Random(42 + size + pattern.GetHashCode());

                byte* compBase, decompBase;
                byte[] compFront, compBack, decompFront, decompBack;

                var compressedPtr = AllocGuarded(maxCompressedSize, rng, out compBase, out compFront, out compBack);
                var decompressedPtr = AllocGuarded(size, rng, out decompBase, out decompFront, out decompBack);
                try
                {
                    fixed (byte* inputPtr = input)
                    {
                        // Compress with OPTIMIZED
                        int compressedSize = LZ4.Encode64(inputPtr, compressedPtr, size, maxCompressedSize);
                        Assert.True(compressedSize > 0, $"Optimized compression failed for size={size}, pattern={pattern}");

                        VerifyGuards(compressedPtr, maxCompressedSize, compFront, compBack, $"compressed buf after LZ4.Encode64 size={size}, pattern={pattern}");

                        // Decompress with OPTIMIZED
                        int decompressedSize = LZ4.Decode64(compressedPtr, compressedSize, decompressedPtr, size, true);
                        Assert.Equal(size, decompressedSize);

                        VerifyGuards(decompressedPtr, size, decompFront, decompBack, $"decompressed buf after LZ4.Decode64 size={size}, pattern={pattern}");

                        // Verify data integrity
                        for (int i = 0; i < size; i++)
                        {
                            Assert.True(input[i] == decompressedPtr[i],
                                $"Data mismatch at position {i} for size={size}, pattern={pattern}. Expected {input[i]}, got {decompressedPtr[i]}");
                        }
                    }
                }
                finally
                {
                    FreeGuarded(compBase);
                    FreeGuarded(decompBase);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Core | RavenTestCategory.Intrinsics)]
        [MemberData(nameof(EdgeCaseSizes))]
        public void CompressionRatioShouldMatch(int size)
        {
            // Skip very small sizes where compression might not occur
            if (size < 13)
                return;

            foreach (var pattern in new[] { "sequential", "highly_compressible" })
            {
                var input = GenerateTestData(size, pattern);
                var maxCompressedSize = LZ4.MaximumOutputLength(size);
                var rng = new Random(42 + size + pattern.GetHashCode());

                byte* compRefBase, compOptBase;
                byte[] compRefFront, compRefBack, compOptFront, compOptBack;

                var compressedRefPtr = AllocGuarded(maxCompressedSize, rng, out compRefBase, out compRefFront, out compRefBack);
                var compressedOptPtr = AllocGuarded(maxCompressedSize, rng, out compOptBase, out compOptFront, out compOptBack);
                try
                {
                    fixed (byte* inputPtr = input)
                    {
                        int refSize = LZ4Reference.Encode64(inputPtr, compressedRefPtr, size, maxCompressedSize);
                        VerifyGuards(compressedRefPtr, maxCompressedSize, compRefFront, compRefBack, $"ref compressed buf size={size}, pattern={pattern}");

                        int optSize = LZ4.Encode64(inputPtr, compressedOptPtr, size, maxCompressedSize);
                        VerifyGuards(compressedOptPtr, maxCompressedSize, compOptFront, compOptBack, $"opt compressed buf size={size}, pattern={pattern}");

                        // Compression ratios should be identical or very close
                        Assert.True(Math.Abs(refSize - optSize) <= Math.Max(1, size / 100),
                            $"Compression ratio mismatch for size={size}, pattern={pattern}. Reference={refSize}, Optimized={optSize}");
                    }
                }
                finally
                {
                    FreeGuarded(compRefBase);
                    FreeGuarded(compOptBase);
                }
            }
        }

        [RavenFact(RavenTestCategory.Core | RavenTestCategory.Intrinsics)]
        public void LimitedOutputMode()
        {
            // Test that limited output mode works correctly
            var size = 10000;
            var input = GenerateTestData(size, "highly_compressible");
            var maxCompressedSize = LZ4.MaximumOutputLength(size);
            var rng = new Random(42 + size);

            byte* compBase, decompBase;
            byte[] compFront, compBack, decompFront, decompBack;

            var compressedPtr = AllocGuarded(maxCompressedSize, rng, out compBase, out compFront, out compBack);
            var decompressedPtr = AllocGuarded(size, rng, out decompBase, out decompFront, out decompBack);
            try
            {
                fixed (byte* inputPtr = input)
                {
                    // Compress with limited output (realistic scenario)
                    int compressedSize = LZ4.Encode64(inputPtr, compressedPtr, size, size - 8);

                    VerifyGuards(compressedPtr, maxCompressedSize, compFront, compBack, "compressed buf after LZ4.Encode64 limited output");

                    if (compressedSize > 0)
                    {
                        // If compression succeeded, verify round-trip
                        int decompressedSize = LZ4.Decode64(compressedPtr, compressedSize, decompressedPtr, size, true);
                        Assert.Equal(size, decompressedSize);

                        VerifyGuards(decompressedPtr, size, decompFront, decompBack, "decompressed buf after LZ4.Decode64 limited output");

                        for (int i = 0; i < size; i++)
                            Assert.Equal(input[i], decompressedPtr[i]);
                    }
                    // If compression failed (returned 0), that's acceptable for limited output mode
                }
            }
            finally
            {
                FreeGuarded(compBase);
                FreeGuarded(decompBase);
            }
        }

        private byte[] GenerateTestData(int size, string pattern)
        {
            var data = new byte[size];
            var rng = new Random(42); // Seeded for reproducibility

            switch (pattern)
            {
                case "zeros":
                    // All zeros - highly compressible
                    Array.Fill(data, (byte)0);
                    break;

                case "ones":
                    // All 0xFF - highly compressible
                    Array.Fill(data, (byte)0xFF);
                    break;

                case "sequential":
                    // Sequential bytes 0,1,2,3... - moderately compressible
                    for (int i = 0; i < size; i++)
                        data[i] = (byte)(i & 0xFF);
                    break;

                case "random_seeded":
                    // Random but reproducible
                    rng.NextBytes(data);
                    break;

                case "highly_compressible":
                    // Repeated patterns - very compressible
                    for (int i = 0; i < size; i++)
                        data[i] = (byte)(i % 4);
                    break;

                case "incompressible":
                    // Random data - essentially incompressible
                    new Random(size).NextBytes(data);
                    break;

                default:
                    throw new ArgumentException($"Unknown pattern: {pattern}");
            }

            return data;
        }
    }
}
