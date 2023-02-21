using System;
using System.Collections.Generic;
using System.Text;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server.Compression;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public class Encoder3GramTests : NoDisposalNeeded
    {
        public Encoder3GramTests(ITestOutputHelper output) : base(output)
        {
        }

        private struct State : IEncoderState
        {
            private readonly byte[] _value;

            public State(int size)
            {
                _value = new byte[size];
            }

            public Span<byte> EncodingTable => new Span<byte>(_value).Slice(0, _value.Length / 2);
            public Span<byte> DecodingTable => new Span<byte>(_value).Slice(_value.Length / 2);
            
            public bool CanGrow => false;

            public void Dispose(){}

            public void Grow(int minimumSize)
            {
                throw new NotSupportedException("This state table does not support growing.");
            }
        }

        private struct StringKeys : IReadOnlySpanIndexer, ISpanIndexer, IReadOnlySpanEnumerator
        {
            private readonly byte[][] _values;
            private int _currentIdx = 0;

            public int Length => _values.Length;

            public bool IsNull(int i)
            {
                if (i < 0 || i >= Length)
                    throw new ArgumentOutOfRangeException();

                return _values[i] == null;
            }

            public ReadOnlySpan<byte> this[int i] => new(_values[i]);

            Span<byte> ISpanIndexer.this[int i] => new(_values[i]);


            public StringKeys(string[] keys)
            {
                _values = new byte[keys.Length][];
                for (int i = 0; i < keys.Length; i++)
                {
                    var value = UTF8Encoding.ASCII.GetBytes(keys[i]);

                    var nullTerminated = new byte[value.Length + 1];
                    nullTerminated[value.Length] = 0;
                    value.AsSpan().CopyTo(nullTerminated);

                    _values[i] = nullTerminated;
                }
            }

            public StringKeys(byte[][] keys)
            {
                _values = keys;
            }

            public void Reset()
            {
                _currentIdx = 0;
            }

            public bool MoveNext(out ReadOnlySpan<byte> result)
            {
                if (_currentIdx >= _values.Length)
                {
                    result = default;
                    return false;
                }

                result = new(_values[_currentIdx++]);
                return true;
            }
        }

        private struct ByteKeys : IReadOnlySpanIndexer, ISpanIndexer, IReadOnlySpanEnumerator
        {
            private readonly byte[][] _values;
            private int _currentIdx = 0;

            public int Length => _values.Length;

            public bool IsNull(int i)
            {
                if (i < 0 || i >= Length)
                    throw new ArgumentOutOfRangeException();

                return _values[i] == null;
            }

            public ReadOnlySpan<byte> this[int i] => new(_values[i]);

            Span<byte> ISpanIndexer.this[int i] => new(_values[i]);

            public ByteKeys(byte[][] keys)
            {
                _values = keys;
            }

            public void Reset()
            {
                _currentIdx = 0;
            }

            public bool MoveNext(out ReadOnlySpan<byte> result)
            {
                if (_currentIdx >= _values.Length)
                {
                    result = default;
                    return false;
                }

                result = _values[_currentIdx++].AsSpan();
                return true;
            }
        }


        private struct EmptyKeys : IReadOnlySpanIndexer, IReadOnlySpanEnumerator
        {
            public int Length => 0;

            public ReadOnlySpan<byte> this[int i] => throw new IndexOutOfRangeException();

            public void Reset()
            {}

            public bool MoveNext(out ReadOnlySpan<byte> result)
            {
                result = default;
                return false;
            }

            public bool IsNull(int i) => throw new IndexOutOfRangeException();
        }

        [Fact]
        public void EmptyDictionaryTrain()
        {
            State state = new(64000);
            var encoder = new HopeEncoder<Encoder3Gram<State>>(new Encoder3Gram<State>(state));

            int dictSize = 128;
            EmptyKeys keys = new();
            encoder.Train(keys, dictSize);

            StringKeys encodingValue = new(new[] { Encoding.ASCII.GetBytes("companies/000000182\0") });

            Span<byte> value = new byte[128];
            Span<byte> decoded = new byte[128];

            var encodedBitLength = encoder.Encode(encodingValue[0], value);
            var decodedBytes = encoder.Decode(value.Slice(0, Bits.ToBytes(encodedBitLength)), decoded);

            Assert.Equal(0, encodingValue[0].SequenceCompareTo(decoded.Slice(0, decodedBytes)));
            Assert.True(encoder.GetMaxEncodingBytes(encodingValue[0].Length) >= encodedBitLength / 8);
        }

        [Fact]
        public void SingleKeyEncoding()
        {
            State state = new(64000);
            var encoder = new HopeEncoder<Encoder3Gram<State>>(new Encoder3Gram<State>(state));

            int rawLength = 0;
            string[] keysAsStrings = new string[10000];
            for (int i = 0; i < keysAsStrings.Length; i++)
            {
                keysAsStrings[i] = $"companies/{i:000000000}";
                rawLength += keysAsStrings[i].Length;
            }

            int dictSize = 128;
            StringKeys keys = new(keysAsStrings);
            encoder.Train(keys, dictSize);

            StringKeys encodingValue = new(new[] { Encoding.ASCII.GetBytes("companies/000000182\0") });

            Span<byte> value = new byte[128];
            Span<byte> decoded = new byte[128];

            var encodedBitLength = encoder.Encode(encodingValue[0], value);
            var decodedBytes = encoder.Decode(value.Slice(0, Bits.ToBytes(encodedBitLength)), decoded);

            Assert.Equal(0, encodingValue[0].SequenceCompareTo(decoded.Slice(0, decodedBytes)));
        }

        [Fact]
        public void SmallValueTrain()
        {
            State state = new(64000);
            var encoder = new HopeEncoder<Encoder3Gram<State>>(new Encoder3Gram<State>(state));

            int rawLength = 0;
            string[] keysAsStrings = new string[10000];
            for (int i = 0; i < keysAsStrings.Length; i++)
            {
                keysAsStrings[i] = $"com";
                rawLength += keysAsStrings[i].Length;
            }

            byte[][] key = new byte[1][];
            key[0] = new byte[] { 60, 61, 62 };

            int dictSize = 128;
            StringKeys keys = new(key);
            encoder.Train(keys, dictSize);

            StringKeys encodingValue = new(new[] { Encoding.ASCII.GetBytes("companies/000000182\0") });

            Span<byte> value = new byte[128];
            Span<byte> decoded = new byte[128];

            var encodedBitLength = encoder.Encode(encodingValue[0], value);
            var decodedBytes = encoder.Decode(value.Slice(0, Bits.ToBytes(encodedBitLength)), decoded);

            Assert.Equal(0, encodingValue[0].SequenceCompareTo(decoded.Slice(0, decodedBytes)));
        }

        public static IEnumerable<object[]> RandomSeed
        {
            get { yield return new object[] { new Random().Next(100000) }; }
        }

        [Theory]
        [MemberData("RandomSeed")]
        public void VerifyOrderPreservation(int randomSeed = 3117)
        {
            State state = new(64000);
            var encoder = new HopeEncoder<Encoder3Gram<State>>(new Encoder3Gram<State>(state));

            var rgn = new Random(randomSeed);

            const int size = 10000;
            int dictSize = rgn.Next(512);

            int rawLength = 0;
            string[] keysAsStrings = new string[size];
            byte[][] outputBuffers = new byte[size][];
            for (int i = 0; i < keysAsStrings.Length; i++)
            {
                int id = rgn.Next(100000000);
                keysAsStrings[i] = $"companies/{id:000000000}";
                outputBuffers[i] = new byte[128];
                rawLength += keysAsStrings[i].Length;
            }

            StringKeys keys = new(keysAsStrings);
            encoder.Train(keys, dictSize);

            // Encode all keys.
            StringKeys inputValues = new(keysAsStrings);
            StringKeys outputValues = new(outputBuffers);
            Span<int> outputValuesSizeInBits = new int[keysAsStrings.Length];
            encoder.Encode(inputValues, outputValues, outputValuesSizeInBits);

            for (int i = 0; i < keysAsStrings.Length * 2; i++)
            {
                var value1Idx = rgn.Next(keysAsStrings.Length - 1);
                var value2Idx = rgn.Next(keysAsStrings.Length - 1);

                var value1 = inputValues[value1Idx];
                var value2 = inputValues[value2Idx];

                var encoded1SizeInBytes = outputValuesSizeInBits[value1Idx] / 8 + (outputValuesSizeInBits[value1Idx] % 8 == 0 ? 0 : 1);
                var encoded2SizeInBytes = outputValuesSizeInBits[value2Idx] / 8 + (outputValuesSizeInBits[value2Idx] % 8 == 0 ? 0 : 1);

                var encodedValue1 = outputValues[value1Idx].Slice(0, encoded1SizeInBytes);
                var encodedValue2 = outputValues[value2Idx].Slice(0, encoded2SizeInBytes);

                var originalOrder = value1.SequenceCompareTo(value2);
                var encodedOrder = encodedValue1.SequenceCompareTo(encodedValue2);

                // Normalize to (-1,0,1)
                originalOrder = (originalOrder < 0) ? -1 : (originalOrder > 0) ? 1 : 0;
                encodedOrder = (encodedOrder < 0) ? -1 : (encodedOrder > 0) ? 1 : 0;

                Assert.Equal(originalOrder, encodedOrder);
            }
        }

        [Theory]
        [MemberData("RandomSeed")]
        public void VerifyOrderPreservationDifferentSizes(int randomSeed = 3117)
        {
            State state = new(64000);
            var encoder = new HopeEncoder<Encoder3Gram<State>>(new Encoder3Gram<State>(state));

            var rgn = new Random(randomSeed);

            const int size = 10000;
            int dictSize = rgn.Next(512);

            string[] keysAsStrings = new string[size];
            byte[][] outputBuffers = new byte[size][];
            for (int i = 0; i < keysAsStrings.Length; i++)
            {
                int id = rgn.Next(100000000);
                keysAsStrings[i] = $"list/{id}";
                outputBuffers[i] = new byte[128];
            }

            StringKeys keys = new(keysAsStrings);
            encoder.Train(keys, dictSize);

            // Encode all keys.
            StringKeys inputValues = new(keysAsStrings);
            StringKeys outputValues = new(outputBuffers);
            Span<int> outputValuesSizeInBits = new int[keysAsStrings.Length];
            encoder.Encode(inputValues, outputValues, outputValuesSizeInBits);

            for (int i = 0; i < keysAsStrings.Length * 2; i++)
            {
                var value1Idx = rgn.Next(keysAsStrings.Length - 1);
                var value2Idx = rgn.Next(keysAsStrings.Length - 1);

                var value1 = inputValues[value1Idx];
                var value2 = inputValues[value2Idx];

                var encoded1SizeInBytes = outputValuesSizeInBits[value1Idx] / 8 + (outputValuesSizeInBits[value1Idx] % 8 == 0 ? 0 : 1);
                var encoded2SizeInBytes = outputValuesSizeInBits[value2Idx] / 8 + (outputValuesSizeInBits[value2Idx] % 8 == 0 ? 0 : 1);

                var encodedValue1 = outputValues[value1Idx].Slice(0, encoded1SizeInBytes);
                var encodedValue2 = outputValues[value2Idx].Slice(0, encoded2SizeInBytes);

                var originalOrder = value1.SequenceCompareTo(value2);
                var encodedOrder = encodedValue1.SequenceCompareTo(encodedValue2);

                // Normalize to (-1,0,1)
                originalOrder = (originalOrder < 0) ? -1 : (originalOrder > 0) ? 1 : 0;
                encodedOrder = (encodedOrder < 0) ? -1 : (encodedOrder > 0) ? 1 : 0;

                Assert.Equal(originalOrder, encodedOrder);
            }
        }

        [Theory]
        [MemberData("RandomSeed")]
        public void VerifyOrderPreservationDifferentSizesWithNulls(int randomSeed = 3117)
        {
            State state = new(64000);
            var encoder = new HopeEncoder<Encoder3Gram<State>>(new Encoder3Gram<State>(state));

            var rgn = new Random(randomSeed);

            const int size = 10000;
            int dictSize = rgn.Next(512);

            string[] keysAsStrings = new string[size];
            byte[][] outputBuffers = new byte[size][];
            for (int i = 0; i < keysAsStrings.Length; i++)
            {
                int id = rgn.Next(100000000); 
                keysAsStrings[i] = $"list\u0000/{id}\u0000";
                outputBuffers[i] = new byte[128];
            }

            StringKeys keys = new(keysAsStrings);
            encoder.Train(keys, dictSize);

            // Encode all keys.
            StringKeys inputValues = new(keysAsStrings);
            StringKeys outputValues = new(outputBuffers);
            Span<int> outputValuesSizeInBits = new int[keysAsStrings.Length];
            encoder.Encode(inputValues, outputValues, outputValuesSizeInBits);

            for (int i = 0; i < keysAsStrings.Length * 2; i++)
            {
                var value1Idx = rgn.Next(keysAsStrings.Length - 1);
                var value2Idx = rgn.Next(keysAsStrings.Length - 1);

                var value1 = inputValues[value1Idx];
                var value2 = inputValues[value2Idx];

                var encoded1SizeInBytes = outputValuesSizeInBits[value1Idx] / 8 + (outputValuesSizeInBits[value1Idx] % 8 == 0 ? 0 : 1);
                var encoded2SizeInBytes = outputValuesSizeInBits[value2Idx] / 8 + (outputValuesSizeInBits[value2Idx] % 8 == 0 ? 0 : 1);

                var encodedValue1 = outputValues[value1Idx].Slice(0, encoded1SizeInBytes);
                var encodedValue2 = outputValues[value2Idx].Slice(0, encoded2SizeInBytes);

                var originalOrder = value1.SequenceCompareTo(value2);
                var encodedOrder = encodedValue1.SequenceCompareTo(encodedValue2);

                // Normalize to (-1,0,1)
                originalOrder = (originalOrder < 0) ? -1 : (originalOrder > 0) ? 1 : 0;
                encodedOrder = (encodedOrder < 0) ? -1 : (encodedOrder > 0) ? 1 : 0;

                Assert.Equal(originalOrder, encodedOrder);
            }
        }

        [Theory]
        [MemberData("RandomSeed")]
        public void VerifyCorrectDecoding(int randomSeed)
        {
            State state = new(64000);
            var encoder = new HopeEncoder<Encoder3Gram<State>>(new Encoder3Gram<State>(state));

            var rgn = new Random(randomSeed);
            const int size = 10000;
            int dictSize = rgn.Next(512);

            int rawLength = 0;
            string[] keysAsStrings = new string[size];
            for (int i = 0; i < keysAsStrings.Length; i++)
            {
                keysAsStrings[i] = $"companies/{i:000000000}";
                rawLength += keysAsStrings[i].Length;
            }

            StringKeys keys = new(keysAsStrings);

            encoder.Train(keys, dictSize);

            Span<byte> value = new byte[128];
            Span<byte> decoded = new byte[128];

            for (int i = 0; i < keys.Length; i++)
            {
                int encodedBitLength = encoder.Encode(keys[i], value);
                var decodedBytes = encoder.Decode(value.Slice(0, Bits.ToBytes(encodedBitLength)), decoded);

                if (keys[i].SequenceCompareTo(decoded.Slice(0, decodedBytes)) != 0)
                {
                    encoder.Encode(keys[i], value);
                    decodedBytes = encoder.Decode(value, decoded);
                }

                Assert.Equal(0, keys[i].SequenceCompareTo(decoded.Slice(0, decodedBytes)));
            }
        }

        [Theory]
        [MemberData("RandomSeed")]
        public void EnsureEscapedSequencesWithNullsWork(int randomSeed)
        {
            State state = new(64000);
            var encoder = new HopeEncoder<Encoder3Gram<State>>(new Encoder3Gram<State>(state));

            var rgn = new Random(randomSeed);
            const int size = 1000;
            int dictSize = rgn.Next(512);

            byte[][] keysAsStrings = new byte[size][];
            for (int i = 0; i < keysAsStrings.Length; i++)
            {
                var key = new byte[rgn.Next(100) + 2];
                rgn.NextBytes(key);
                key[^1] = 0; // It has to be null terminated. 

                key[rgn.Next(key.Length - 1)] = 0; // Plant a null in between. 

                keysAsStrings[i] = key;
            }

            ByteKeys keys = new(keysAsStrings);

            encoder.Train(keys, dictSize);

            Span<byte> value = new byte[128];
            Span<byte> decoded = new byte[128];

            var escape = new byte[] { 0 };
            int lengthInBits = encoder.Encode(escape, value);
            var decodedBytes = encoder.Decode(lengthInBits, value, decoded);
            Assert.Equal(0, escape.AsSpan().SequenceCompareTo(decoded.Slice(0, decodedBytes)));

            escape = new byte[] { 0, 0 };
            lengthInBits = encoder.Encode(escape, value);
            decodedBytes = encoder.Decode(lengthInBits, value, decoded);
            Assert.Equal(0, escape.AsSpan().SequenceCompareTo(decoded.Slice(0, decodedBytes)));

            escape = new byte[] { 0, 1 };
            lengthInBits = encoder.Encode(escape, value);
            decodedBytes = encoder.Decode(lengthInBits, value, decoded);
            Assert.Equal(0, escape.AsSpan().SequenceCompareTo(decoded.Slice(0, decodedBytes)));

            escape = new byte[] { 0, 1, 1 };
            lengthInBits = encoder.Encode(escape, value);
            decodedBytes = encoder.Decode(lengthInBits, value, decoded);
            Assert.Equal(0, escape.AsSpan().SequenceCompareTo(decoded.Slice(0, decodedBytes)));

            escape = new byte[] { 1, 1, 1 };
            lengthInBits = encoder.Encode(escape, value);
            decodedBytes = encoder.Decode(lengthInBits, value, decoded);
            Assert.Equal(0, escape.AsSpan().SequenceCompareTo(decoded.Slice(0, decodedBytes)));

            escape = new byte[] { 0, 1, 1, 1 };
            lengthInBits = encoder.Encode(escape, value);
            decodedBytes = encoder.Decode(lengthInBits, value, decoded);
            Assert.Equal(0, escape.AsSpan().SequenceCompareTo(decoded.Slice(0, decodedBytes)));
        }

        [Theory]
        [MemberData("RandomSeed")]
        public void VerifyCorrectDecodingWithNulls(int randomSeed)
        {
            State state = new(64000);
            var encoder = new HopeEncoder<Encoder3Gram<State>>(new Encoder3Gram<State>(state));

            var rgn = new Random(randomSeed);
            const int size = 1000;
            int dictSize = rgn.Next(512);

            byte[][] keysAsStrings = new byte[size][];
            for (int i = 0; i < keysAsStrings.Length; i++)
            {
                var key = new byte[rgn.Next(100)+2];
                rgn.NextBytes(key);
                key[^1] = 0; // It has to be null terminated. 

                key[rgn.Next(key.Length - 1)] = 0; // Plant a null in between. 

                keysAsStrings[i] = key;
            }

            ByteKeys keys = new(keysAsStrings);

            encoder.Train(keys, dictSize);

            Span<byte> value = new byte[128];
            Span<byte> decoded = new byte[128];

            for (int i = 0; i < keysAsStrings.Length; i++)
            {
                var key = keysAsStrings[i];

                int lengthInBits = encoder.Encode(key, value);
                var decodedBytes = encoder.Decode(lengthInBits, value, decoded);

                if (keys[i].SequenceCompareTo(decoded.Slice(0, decodedBytes)) != 0)
                {
                    lengthInBits = encoder.Encode(key, value);
                    decodedBytes = encoder.Decode(lengthInBits, value, decoded);
                }

                Assert.Equal(0, key.AsSpan().SequenceCompareTo(decoded.Slice(0, decodedBytes)));
            }
        }

        [Fact]
        public void EncodingNullValues()
        {
            // We train an encoder with some random stuff.
            State state = new(64000);
            var encoder = new HopeEncoder<Encoder3Gram<State>>(new Encoder3Gram<State>(state));

            var rgn = new Random(1337);
            const int size = 10000;
            int dictSize = rgn.Next(512);

            byte[][] keysAsStrings = new byte[size][];
            for (int i = 0; i < keysAsStrings.Length; i++)
            {
                var key = new byte[rgn.Next(100)];
                rgn.NextBytes(key);

                keysAsStrings[i] = key;
            }

            ByteKeys keys = new(keysAsStrings);
            encoder.Train(keys, dictSize);

            Span<byte> encoded = new byte[128];
            Span<byte> decoded = new byte[128];

            Span<byte> value = new byte[128];
            value.Fill(0); // We write the whole value with nulls.
            for (int i = 2; i < 20; i++)
            {
                var key = value[..i];

                int length = encoder.Encode(key, encoded);
                var decodedBytes = encoder.Decode(length, encoded, decoded);

                if (key.SequenceCompareTo(decoded.Slice(0, decodedBytes)) != 0)
                {
                    length = encoder.Encode(key, encoded);
                    decodedBytes = encoder.Decode(encoded.Slice(0, Bits.ToBytes(length)), decoded);
                }

                Assert.Equal(0, key.SequenceCompareTo(decoded[..decodedBytes]));
            }
        }

        [Fact]
        public void NullsAtTheEndWithDifferentSizes()
        {
            State state = new(64000);
            var encoder = new HopeEncoder<Encoder3Gram<State>>(new Encoder3Gram<State>(state));

            int dictSize = 128;
            EmptyKeys keys = new();
            encoder.Train(keys, dictSize);

            StringKeys encodingValue = new(new[] { Encoding.ASCII.GetBytes("companies/000000182") });

            Span<byte> value = new byte[128];
            Span<byte> decoded = new byte[128];

            var encodedBitLength = encoder.Encode(encodingValue[0], value);
            
            var decodedBytes = encoder.Decode(Bits.ToBytes(encodedBitLength) * 8, value, decoded);

            Assert.Equal(0, encodingValue[0].SequenceCompareTo(decoded.Slice(0, decodedBytes)));
            Assert.Equal(encodingValue[0].Length, decodedBytes);
            Assert.True(encoder.GetMaxEncodingBytes(encodingValue[0].Length) >= encodedBitLength / 8);
        }
    }
}
