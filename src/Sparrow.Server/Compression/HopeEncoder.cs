using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
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

        [SkipLocalsInit]
        public int Encode(in ReadOnlySpan<byte> data, in Span<byte> outputBuffer)
        {
            // When we support NULL values and try to store only the necessary bytes instead of the bits, we face a challenge.
            // If we store the bits length, we may need to use an extra byte when the key length is greater than 15 bytes,
            // even though we could use a single byte for up to 127 bytes if we quantize.However, since the scenario of storing
            // keys with nulls is very uncommon (although it can occur), we have to choose between paying for a little more
            // complexity and instructions versus using bigger storage for very common key lengths.

            // In most cases, null values are handled seamlessly during the decoding process. However, if the key ends with a
            // NULL value and certain conditions are met, the decoding process may return an extra null value. Since we do not
            // know the original size of the array, we cannot disambiguate the size. To address this issue,
            // we escape the last '\0' value by adding an extra '\1' value.
            //
            // As a result, the following mapping is applied:
            //      0 -> 0 1
            //    0 1 -> 0 1 1
            //  0 1 1 -> 0 1 1 1
            //  and so on.

            for (int i = data.Length - 1; i >= 0; i--)
            {
                byte value = data[i];
                if (value >= 2)
                    goto Unescaped;
                if (value == 0)
                    goto Unlikely;
            }

            Unescaped:
            return _encoder.Encode(data, outputBuffer);

            Unlikely:
            return EncodeEscapedUnlikely(data, outputBuffer);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private int EncodeEscapedUnlikely(in ReadOnlySpan<byte> data, in Span<byte> outputBuffer)
        {
            var pool = ArrayPool<byte>.Shared;
            var tmp = pool.Rent(data.Length + 1);
            data.CopyTo(tmp);
            tmp[data.Length] = 1;

            var bitsUsed = _encoder.Encode(tmp.AsSpan(0, data.Length + 1), outputBuffer);
            pool.Return(tmp);

            return bitsUsed;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Decode(in ReadOnlySpan<byte> data, in Span<byte> outputBuffer)
        {
            return Decode(data.Length * 8, data, outputBuffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Decode(int bits, in ReadOnlySpan<byte> data, in Span<byte> outputBuffer)
        {
            int length = _encoder.Decode(bits, data, outputBuffer);

            // Since we may have already escaped the data, we need to detect such a condition. 
            // If any of the following conditions is met, then the inverse mapping is applied:
            //     0 1 -> 0  
            //   0 1 1 -> 0 1
            // 0 1 1 1 -> 0 1 1 
            // and so on.

            // In order to address this issue, we will remove only the last `\1`.
            for (int i = length - 1; i >= 0; i--)
            {
                byte value = outputBuffer[i];
                if (value >= 2)
                    return length;
                if (value == 0)
                    return length - 1;
            }

            return length;
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

        [DoesNotReturn]
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
