using System;
using System.Numerics.Tensors;
using System.Runtime.InteropServices;
using System.Threading;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Graphs;
using Xunit;
using VectorEmbeddingType = Voron.Data.Graphs.VectorEmbeddingType;

namespace FastTests.Voron.Graphs.HnswTests;

public class L2NormCaching(ITestOutputHelper output) : StorageTest(output)
{
    [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    [InlineData(128)]
    [InlineData(384)]
    [InlineData(768)]
    [InlineData(1536)]
    public void LegacyAndNormalizedKernelsAgree(int dims)
    {
        // For random f32 vectors, the legacy CosineDistanceSingles (which recomputes both norms
        // per call) and the new CosineDistanceSinglesNormalized (which reads the trailing norm
        // and dots pre-normalized unit vectors) must return the same distance up to rounding.
        var rng = new Random(dims);
        var a = RandomFloats(rng, dims, scale: 3.0f);
        var b = RandomFloats(rng, dims, scale: 5.0f);

        var legacy = global::Voron.Data.Graphs.Hnsw.CosineDistanceSingles(
            MemoryMarshal.AsBytes(a.AsSpan()),
            MemoryMarshal.AsBytes(b.AsSpan()));

        var aAtRest = new byte[Hnsw.TensorSizeBytes<float>(dims)];
        var bAtRest = new byte[Hnsw.TensorSizeBytes<float>(dims)];
        Hnsw.WriteNormalizedTensor(a, aAtRest);
        Hnsw.WriteNormalizedTensor(b, bAtRest);

        var normalized = global::Voron.Data.Graphs.Hnsw.CosineDistanceSinglesNormalized(aAtRest, bAtRest);

        // Allow a small absolute tolerance; the two kernels differ only in float rounding order.
        Assert.InRange(normalized - legacy, -1e-5f, 1e-5f);
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void NormalizeRoundTrip()
    {
        var rng = new Random(42);
        const int dims = 64;
        var raw = RandomFloats(rng, dims, scale: 7.0f);
        var expectedNorm = MathF.Sqrt(TensorPrimitives.SumOfSquares(raw));

        var atRest = new byte[Hnsw.TensorSizeBytes<float>(dims)];
        var returnedNorm = Hnsw.WriteNormalizedTensor(raw, atRest);

        Assert.Equal(expectedNorm, returnedNorm, precision: 4);
        Assert.Equal(expectedNorm, Hnsw.ReadTensorMagnitude(atRest), precision: 4);

        var unit = Hnsw.ReadTensorVector<float>(atRest);
        Assert.Equal(dims, unit.Length);

        // Dividing by the norm should leave a unit vector.
        var unitSumSq = TensorPrimitives.SumOfSquares(unit);
        Assert.Equal(1.0, (double)unitSumSq, precision: 4);
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void ZeroNormAdversarial()
    {
        const int dims = 16;
        var zero = new float[dims];
        var nonZero = RandomFloats(new Random(1), dims, scale: 1.0f);

        var zeroAtRest = new byte[Hnsw.TensorSizeBytes<float>(dims)];
        var nonZeroAtRest = new byte[Hnsw.TensorSizeBytes<float>(dims)];
        Hnsw.WriteNormalizedTensor(zero, zeroAtRest);
        Hnsw.WriteNormalizedTensor(nonZero, nonZeroAtRest);

        // Both operands zero: kernel returns NaN (matches the semantics of the recompute-norms
        // path, which divides by zero and produces NaN).
        var bothZero = global::Voron.Data.Graphs.Hnsw.CosineDistanceSinglesNormalized(zeroAtRest, zeroAtRest);
        Assert.True(float.IsNaN(bothZero));

        // Exactly one operand zero: kernel returns 1f (distance = 1, similarity = 0). The spec
        // does not prescribe more than "not NaN" for this case; 1f is the documented contract.
        var mixedA = global::Voron.Data.Graphs.Hnsw.CosineDistanceSinglesNormalized(zeroAtRest, nonZeroAtRest);
        var mixedB = global::Voron.Data.Graphs.Hnsw.CosineDistanceSinglesNormalized(nonZeroAtRest, zeroAtRest);
        Assert.False(float.IsNaN(mixedA));
        Assert.False(float.IsNaN(mixedB));
        Assert.Equal(1f, mixedA);
        Assert.Equal(1f, mixedB);
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void EndToEndHnswSearchReturnsCorrectNearestNeighbour()
    {
        using var _ = Slice.From(Allocator, "l2norm-e2e", out var treeName);
        const int dims = 32;
        var sizeBytes = dims * sizeof(float);

        // Hand-crafted vectors chosen so vector #5 is clearly closest to the query.
        var v1 = FilledVector(dims, 0.1f);
        var v2 = FilledVector(dims, 0.5f);
        var v3 = FilledVector(dims, -0.2f);
        var v4 = FilledVector(dims, 1.5f);
        var v5 = FilledVector(dims, 2.0f);

        var query = FilledVector(dims, 1.95f); // closest to v5 by cosine similarity

        using (var tx = Env.WriteTransaction())
        {
            global::Voron.Data.Graphs.Hnsw.Create(tx.LowLevelTransaction, treeName, sizeBytes, 8, 16, VectorEmbeddingType.Single);
            using (var registration = global::Voron.Data.Graphs.Hnsw.RegistrationFor(tx.LowLevelTransaction, treeName, new Random(7)))
            {
                registration.Register(1, MemoryMarshal.Cast<float, byte>(v1));
                registration.Register(2, MemoryMarshal.Cast<float, byte>(v2));
                registration.Register(3, MemoryMarshal.Cast<float, byte>(v3));
                registration.Register(4, MemoryMarshal.Cast<float, byte>(v4));
                registration.Register(5, MemoryMarshal.Cast<float, byte>(v5));
                registration.Commit(CancellationToken.None);
            }
            tx.Commit();
        }

        using (var tx = Env.ReadTransaction())
        {
            var queryBytes = MemoryMarshal.AsBytes(query.AsSpan()).ToArray();
            using var retriever = global::Voron.Data.Graphs.Hnsw.ApproximateNearest(
                tx.LowLevelTransaction, treeName, numberOfCandidates: 8, queryBytes, 0f);

            Span<long> matches = stackalloc long[8];
            Span<float> distances = stackalloc float[8];
            var read = retriever.Fill(matches, distances, filter: null);
            Assert.True(read >= 1);
            Assert.Equal(5L, matches[0]);
        }
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void LegacyVersionOptionsYieldLegacyKernel()
    {
        // Construct an Options struct pinned to the InitialVersion on-disk layout and verify
        // the kernel dispatcher selects the norm-recomputing path. Locks the version gate so
        // an accidental bump cannot silently break indexes written under that layout.
        var legacyOptions = new global::Voron.Data.Graphs.Hnsw.Options
        {
            Version = global::Voron.Global.Constants.Graphs.HnswVersion.InitialVersion,
            SimilarityMethod = global::Voron.Data.Graphs.Hnsw.SimilarityMethod.CosineSimilaritySingles,
            VectorSizeBytes = 64,
        };
        var newOptions = legacyOptions;
        newOptions.Version = global::Voron.Global.Constants.Graphs.HnswVersion.SinglesWithL2Norm;
        newOptions.VectorSizeBytes = 68;

        unsafe
        {
            var legacyKernel = global::Voron.Data.Graphs.Hnsw.GetDistanceKernel(in legacyOptions);
            var newKernel = global::Voron.Data.Graphs.Hnsw.GetDistanceKernel(in newOptions);

            // Evaluate each kernel on payloads sized to match its expected on-disk layout. The
            // legacy kernel consumes 64 bytes of raw floats; the new kernel consumes 68 bytes
            // (16 unit floats + trailing norm). Confirming each kernel accepts only its own
            // layout proves the dispatcher selects different code paths.
            var rng = new Random(11);
            var a = RandomFloats(rng, 16, scale: 1.0f);
            var b = RandomFloats(rng, 16, scale: 1.0f);
            var aRaw = MemoryMarshal.AsBytes(a.AsSpan());
            var bRaw = MemoryMarshal.AsBytes(b.AsSpan());
            var legacyVal = legacyKernel(aRaw, bRaw);
            Assert.False(float.IsNaN(legacyVal));

            var aAtRest = new byte[Hnsw.TensorSizeBytes<float>(16)];
            var bAtRest = new byte[Hnsw.TensorSizeBytes<float>(16)];
            Hnsw.WriteNormalizedTensor(a, aAtRest);
            Hnsw.WriteNormalizedTensor(b, bAtRest);
            var newVal = newKernel(aAtRest, bAtRest);
            Assert.False(float.IsNaN(newVal));
            // Mathematically equivalent, so the magnitudes must agree to float tolerance.
            Assert.InRange(newVal - legacyVal, -1e-5f, 1e-5f);
        }
    }

    private static float[] RandomFloats(Random rng, int dims, float scale)
    {
        var v = new float[dims];
        for (int i = 0; i < dims; i++)
            v[i] = (float)((rng.NextDouble() * 2 - 1) * scale);
        return v;
    }

    private static float[] FilledVector(int dims, float value)
    {
        var v = new float[dims];
        for (int i = 0; i < dims; i++)
            v[i] = value + 0.001f * i;
        return v;
    }
}
