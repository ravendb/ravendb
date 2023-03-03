using System;

namespace Sparrow.Server.Compression
{
    public interface IEncoderAlgorithm : IDisposable
    {
        public int MaxBitSequenceLength { get; }
        public int MinBitSequenceLength { get; }

        void Train<TSampleEnumerator>(in TSampleEnumerator enumerator, int dictionarySize)
            where TSampleEnumerator : struct, IReadOnlySpanEnumerator;

        void EncodeBatch<TSampleEnumerator, TOutputEnumerator>(in TSampleEnumerator data, Span<int> outputSizes, in TOutputEnumerator outputBuffer)
            where TSampleEnumerator : struct, IReadOnlySpanIndexer
            where TOutputEnumerator : struct, ISpanIndexer;

        void DecodeBatch<TSampleEnumerator, TOutputEnumerator>(in TSampleEnumerator data, Span<int> outputSize, in TOutputEnumerator outputBuffer)
            where TSampleEnumerator : struct, IReadOnlySpanIndexer
            where TOutputEnumerator : struct, ISpanIndexer;

        void DecodeBatch<TSampleEnumerator, TOutputEnumerator>(ReadOnlySpan<int> bits, in TSampleEnumerator data, Span<int> outputSize, in TOutputEnumerator outputBuffer)
            where TSampleEnumerator : struct, IReadOnlySpanIndexer
            where TOutputEnumerator : struct, ISpanIndexer;


        int Encode(ReadOnlySpan<byte> data, Span<byte> outputBuffer);

        int Decode(ReadOnlySpan<byte> data, Span<byte> outputBuffer);

        int Decode(int bits, ReadOnlySpan<byte> data, Span<byte> outputBuffer);
    }
}
