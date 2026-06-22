using System;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Server;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    /// <summary>
    /// Shared storage layout for HNSW tensors that carry a scalar magnitude: <c>T[dims]</c>
    /// followed by a trailing <c>float</c>. Used by <see cref="SimilarityMethod.CosineSimilaritySingles"/>
    /// (unit vector + L2 norm, starting at <see cref="Global.Constants.Graphs.HnswVersion.SinglesWithL2Norm"/>)
    /// and <see cref="SimilarityMethod.CosineSimilarityI8"/> (int8-quantized vector + quantization
    /// scale). Hamming/Binary does not use this layout.
    /// </summary>
    public const int MagnitudeSizeInBytes = sizeof(float);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int TensorSizeBytes<T>(int dimensions) where T : unmanaged
        => dimensions * Unsafe.SizeOf<T>() + MagnitudeSizeInBytes;

    /// <summary>
    /// Writes <paramref name="vector"/> followed by <paramref name="magnitude"/> into
    /// <paramref name="destination"/>. The destination must be sized exactly
    /// <see cref="TensorSizeBytes{T}"/>.
    /// </summary>
    public static void WriteTensor<T>(ReadOnlySpan<T> vector, float magnitude, Span<byte> destination) where T : unmanaged
    {
        int expected = TensorSizeBytes<T>(vector.Length);
        if (destination.Length != expected)
            throw new ArgumentException($"Destination must be {expected} bytes, got {destination.Length}.", nameof(destination));

        var vecBytes = MemoryMarshal.AsBytes(vector);
        vecBytes.CopyTo(destination);
        Unsafe.WriteUnaligned(ref destination[vecBytes.Length], magnitude);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static float ReadTensorMagnitude(ReadOnlySpan<byte> atRest)
        => Unsafe.ReadUnaligned<float>(
            ref MemoryMarshal.GetReference(atRest[^MagnitudeSizeInBytes..]));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ReadOnlySpan<T> ReadTensorVector<T>(ReadOnlySpan<byte> atRest) where T : unmanaged
        => MemoryMarshal.Cast<byte, T>(atRest[..^MagnitudeSizeInBytes]);

    /// <summary>
    /// True when vectors for <paramref name="options"/> must be pre-normalized into the storage
    /// layout before being handed to HNSW. Only the f32 cosine path needs this; Int8 and Hamming
    /// keep their original shapes.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool VectorNormalizationRequired(in Options options) =>
        options.SimilarityMethod == SimilarityMethod.CosineSimilaritySingles
        && options.Version >= Global.Constants.Graphs.HnswVersion.SinglesWithL2Norm;

    /// <summary>
    /// Allocates a scope-owned buffer, normalizes <paramref name="vector"/> into it, and
    /// rebinds <paramref name="vector"/> to the new buffer. Returns <c>false</c> without
    /// allocating when no normalization is needed (similarity method that does not consume
    /// the normalized layout, on-disk version that predates it, or input already in storage
    /// form). Throws when the input is not a raw
    /// float[dims] payload of the expected dimensionality.
    /// </summary>
    internal static bool TryNormalizeQueryVector(in Options options, ByteStringContext allocator, ref Memory<byte> vector,
        out ByteStringContext<ByteStringMemoryCache>.InternalScope scope)
    {
        scope = default;
        if (VectorNormalizationRequired(options) == false)
            return false;
        if (vector.Length == options.VectorSizeBytes)
            return false;

        var floats = MemoryMarshal.Cast<byte, float>(vector.Span);
        var expected = TensorSizeBytes<float>(floats.Length);
        if (expected != options.VectorSizeBytes)
            throw new ArgumentException($"Query vector has {vector.Length} bytes which does not match the expected normalized size ({options.VectorSizeBytes}) for this index.");

        scope = AllocateNormalizedQueryVector(allocator, floats, expected, out vector);
        return true;
    }

    private static unsafe ByteStringContext<ByteStringMemoryCache>.InternalScope AllocateNormalizedQueryVector(
        ByteStringContext allocator, ReadOnlySpan<float> floats, int expected, out Memory<byte> vector)
    {
        var scope = allocator.Allocate(expected, out ByteString buf);
        WriteNormalizedTensor(floats, buf.ToSpan());
        vector = new NormalizedQueryMemoryManager(buf.Ptr, expected).Memory;
        return scope;
    }

    private sealed unsafe class NormalizedQueryMemoryManager : System.Buffers.MemoryManager<byte>
    {
        private readonly byte* _ptr;
        private readonly int _length;

        public NormalizedQueryMemoryManager(byte* ptr, int length)
        {
            _ptr = ptr;
            _length = length;
        }

        public override Span<byte> GetSpan() => new(_ptr, _length);
        public override System.Buffers.MemoryHandle Pin(int elementIndex = 0) => new(_ptr + elementIndex);
        public override void Unpin() { }
        protected override void Dispose(bool disposing) { }
    }

    /// <summary>
    /// Normalizes <paramref name="raw"/> in place into <paramref name="destination"/>
    /// (unit floats + trailing L2 norm). Returns the original L2 norm; zero-norm input
    /// is preserved verbatim with magnitude 0 and the distance kernel must branch on it.
    /// </summary>
    public static float WriteNormalizedTensor(ReadOnlySpan<float> raw, Span<byte> destination)
    {
        int expected = TensorSizeBytes<float>(raw.Length);
        if (destination.Length != expected)
            throw new ArgumentException($"Destination must be {expected} bytes, got {destination.Length}.", nameof(destination));

        var unit = MemoryMarshal.Cast<byte, float>(destination[..(raw.Length * sizeof(float))]);

        float sumSq = TensorPrimitives.SumOfSquares(raw);
        float magnitude = MathF.Sqrt(sumSq);
        if (magnitude > 0f)
            TensorPrimitives.Divide(raw, magnitude, unit);
        else
            raw.CopyTo(unit); // zero vector: preserve zeros; kernel must branch on magnitude == 0

        Unsafe.WriteUnaligned(
            ref destination[raw.Length * sizeof(float)],
            magnitude);

        return magnitude;
    }
}
