using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Server.Compression
{
    public sealed class HopeEncoder<TAlgorithm> : IDisposable
        where TAlgorithm : struct, IEncoderAlgorithm
    {
        private TAlgorithm _encoder;
        private bool _isTrained;

        private int _maxSequenceLength;
        private int _maxSequenceLengthMultiplier;
        private int _minSequenceLength;
        private int _minSequenceLengthMultiplier;

        public HopeEncoder(TAlgorithm encoder = default)
        {
            _encoder = encoder;
            _maxSequenceLength = _encoder.MaxBitSequenceLength;
            _maxSequenceLengthMultiplier = (int)(_maxSequenceLength / 8.0) + 1;

            _minSequenceLength = _encoder.MinBitSequenceLength;
            _minSequenceLengthMultiplier = ((int)(8 / (float)_minSequenceLength) + 1) * 3;

            if (_minSequenceLength is < 1 or > 128 || _maxSequenceLength < 1)
                _isTrained = false;
            else
                _isTrained = true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Train<TSampleEnumerator>(in TSampleEnumerator enumerator, int dictionarySize)
            where TSampleEnumerator : struct, IReadOnlySpanEnumerator
        {
            _encoder.Train(enumerator, dictionarySize);
            _maxSequenceLength = _encoder.MaxBitSequenceLength;
            _maxSequenceLengthMultiplier = (int)(_maxSequenceLength / 8.0) + 1;

            _minSequenceLength = _encoder.MinBitSequenceLength;
            _minSequenceLengthMultiplier = ((int)(8 / (float)_minSequenceLength) + 1) * 3;

            if (_minSequenceLength is < 1 or > 128 || _maxSequenceLength < 1)
                _isTrained = false;
            else
                _isTrained = true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Encode<TSource, TDestination>(in TSource inputBuffers, in TDestination outputBuffers, Span<int> outputSizes)
            where TSource : struct, IReadOnlySpanIndexer
            where TDestination : struct, ISpanIndexer
        {
            if (outputBuffers.Length != outputSizes.Length)
                throw new ArgumentException($"'{nameof(outputBuffers)}' and '{nameof(outputSizes)}' must be of the same size.");

            if (outputBuffers.Length != inputBuffers.Length)
                throw new ArgumentException($"'{nameof(outputBuffers)}' and '{nameof(inputBuffers)}' must be of the same size.");

            _encoder.EncodeBatch(in inputBuffers, outputSizes, in outputBuffers);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Decode<TSource, TDestination>(in TSource inputBuffers, in TDestination outputBuffers, Span<int> outputSizes)
            where TSource : struct, IReadOnlySpanIndexer
            where TDestination : struct, ISpanIndexer
        {
            if (outputBuffers.Length != outputSizes.Length)
                throw new ArgumentException($"'{nameof(outputBuffers)}' and '{nameof(outputSizes)}' must be of the same size.");

            if (outputBuffers.Length != inputBuffers.Length)
                throw new ArgumentException($"'{nameof(outputBuffers)}' and '{nameof(inputBuffers)}' must be of the same size.");

            _encoder.DecodeBatch(in inputBuffers, outputSizes, in outputBuffers);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Encode(in ReadOnlySpan<byte> data, in Span<byte> outputBuffer)
        {
            return _encoder.Encode(data, outputBuffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Decode(in ReadOnlySpan<byte> data, in Span<byte> outputBuffer)
        {
            return _encoder.Decode(data, outputBuffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Decode(int bits, in ReadOnlySpan<byte> data, in Span<byte> outputBuffer)
        {
            return _encoder.Decode(bits, data, outputBuffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetMaxEncodingBytes(int keySize)
        {
            if (!_isTrained)
                ThrowInvalidOperationWithoutTrainedDictionary();

            int value = _maxSequenceLengthMultiplier * keySize;
            return value % sizeof(long) == 0 ? value : value + sizeof(long);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetMaxDecodingBytes(int keySize)
        {
            if (!_isTrained)
                ThrowInvalidOperationWithoutTrainedDictionary();

            return _minSequenceLengthMultiplier * keySize;
        }

        private static void ThrowInvalidOperationWithoutTrainedDictionary()
        {
            throw new InvalidOperationException("Cannot calculate without a trained dictionary");
        }

        public void Dispose()
        {
            _encoder.Dispose();
        }
    }
}
