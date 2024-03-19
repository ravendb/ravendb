using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using FastTests.Voron.FixedSize;
using Raven.Server.Documents.Indexes.Static;
using Sparrow;
using Tests.Infrastructure;
using Voron.Data.CompactTrees;
using Voron.Global;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.CompactTrees
{
    public class RavenDB_21521 : StorageTest
    {
        public RavenDB_21521(ITestOutputHelper output) : base(output)
        {
        }

        public class RandomTrainIterator : IReadOnlySpanEnumerator
        {
            private ReadOnlySpan<byte> Chars => "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"u8;
            private Random _generator;
            private int _currentIdx;
            private int _maxSize;


            public RandomTrainIterator(int count, int maxSize, int seed)
            {
                _generator = new Random(seed);

                _currentIdx = 0;
                _maxSize = maxSize;

                Count = count;
            }

            public int Count { get; }

            public void Reset()
            {
                _currentIdx = 0;
            }

            public bool MoveNext(out ReadOnlySpan<byte> result)
            {
                if (_currentIdx > Count)
                {
                    result = ReadOnlySpan<byte>.Empty;
                    return false;
                }

                var output = new byte[_generator.Next(_maxSize)];

                for (int i = 0; i < output.Length; i++)
                    output[i] = Chars[_generator.Next(Chars.Length)];

                _currentIdx++;

                result = output.AsSpan();
                return true;
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineDataWithRandomSeed]
        public void CompactKeyFuzzy(int seed)
        {
            using var wtx = Env.WriteTransaction();

            var dict1 = PersistentDictionary.CreateDefault(wtx.LowLevelTransaction);
            Assert.True(PersistentDictionary.TryCreate(wtx.LowLevelTransaction, new RandomTrainIterator(1024, 30, seed), out var dict2));
            Assert.True(PersistentDictionary.TryCreate(wtx.LowLevelTransaction, new RandomTrainIterator(1024, 30, seed), out var dict3));
            Assert.True(PersistentDictionary.TryCreate(wtx.LowLevelTransaction, new RandomTrainIterator(1024, 30, seed), out var dict4));
            Assert.True(PersistentDictionary.TryCreate(wtx.LowLevelTransaction, new RandomTrainIterator(1024, 30, seed), out var dict5));

            var key = new CompactKey();
            key.Initialize(wtx.LowLevelTransaction);
            
            var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"u8;
            var random = new Random(seed);

            var stringChars = new byte[2 * Constants.CompactTree.MaximumKeySize];
            for (int i = 0; i < stringChars.Length; i++)
                stringChars[i] = chars[random.Next(chars.Length)];

            int j = 1;
            while (j < stringChars.Length)
            {
                key.Set(stringChars[..j]);
                key.ChangeDictionary(dict1);

                Assert.Equal(stringChars[..j], key.Decoded());

                var dict1EncodedKey = key.EncodedWith(dict1, out var dict1LengthInBits);
                var dict2EncodedKey = key.EncodedWith(dict2.DictionaryId, out var dict2LengthInBits);
                var dict3EncodedKey = key.EncodedWith(dict3.DictionaryId, out var dict3LengthInBits);
                var dict4EncodedKey = key.EncodedWith(dict4.DictionaryId, out var dict4LengthInBits);
                var dict5EncodedKey = key.EncodedWith(dict5.DictionaryId, out var dict5LengthInBits);

                Assert.Equal(0, key.CompareEncodedWith(ref MemoryMarshal.GetReference(dict1EncodedKey), dict1LengthInBits, dict1));
                Assert.Equal(0, key.CompareEncodedWith(ref MemoryMarshal.GetReference(dict2EncodedKey), dict2LengthInBits, dict2.DictionaryId));
                Assert.Equal(0, key.CompareEncodedWith(ref MemoryMarshal.GetReference(dict3EncodedKey), dict3LengthInBits, dict3.DictionaryId));
                Assert.Equal(0, key.CompareEncodedWith(ref MemoryMarshal.GetReference(dict4EncodedKey), dict4LengthInBits, dict4.DictionaryId));
                Assert.Equal(0, key.CompareEncodedWith(ref MemoryMarshal.GetReference(dict5EncodedKey), dict5LengthInBits, dict5.DictionaryId));

                key.ChangeDictionary(dict2.DictionaryId);

                Assert.Equal(0, key.CompareEncodedWith(ref MemoryMarshal.GetReference(dict1EncodedKey), dict1LengthInBits, dict1));
                Assert.Equal(0, key.CompareEncodedWith(ref MemoryMarshal.GetReference(dict2EncodedKey), dict2LengthInBits, dict2.DictionaryId));
                Assert.Equal(0, key.CompareEncodedWith(ref MemoryMarshal.GetReference(dict3EncodedKey), dict3LengthInBits, dict3.DictionaryId));
                Assert.Equal(0, key.CompareEncodedWith(ref MemoryMarshal.GetReference(dict4EncodedKey), dict4LengthInBits, dict4.DictionaryId));
                Assert.Equal(0, key.CompareEncodedWith(ref MemoryMarshal.GetReference(dict5EncodedKey), dict5LengthInBits, dict5.DictionaryId));

                Assert.Equal(stringChars[..j], key.Decoded());

                key.ChangeDictionary(dict3.DictionaryId);

                Assert.Equal(0, key.CompareEncodedWith(ref MemoryMarshal.GetReference(dict1EncodedKey), dict1LengthInBits, dict1));
                Assert.Equal(0, key.CompareEncodedWith(ref MemoryMarshal.GetReference(dict2EncodedKey), dict2LengthInBits, dict2.DictionaryId));
                Assert.Equal(0, key.CompareEncodedWith(ref MemoryMarshal.GetReference(dict3EncodedKey), dict3LengthInBits, dict3.DictionaryId));
                Assert.Equal(0, key.CompareEncodedWith(ref MemoryMarshal.GetReference(dict4EncodedKey), dict4LengthInBits, dict4.DictionaryId));
                Assert.Equal(0, key.CompareEncodedWith(ref MemoryMarshal.GetReference(dict5EncodedKey), dict5LengthInBits, dict5.DictionaryId));

                Assert.Equal(stringChars[..j], key.Decoded());

                key.ChangeDictionary(dict4.DictionaryId);

                Assert.Equal(0, key.CompareEncodedWith(ref MemoryMarshal.GetReference(dict1EncodedKey), dict1LengthInBits, dict1));
                Assert.Equal(0, key.CompareEncodedWith(ref MemoryMarshal.GetReference(dict2EncodedKey), dict2LengthInBits, dict2.DictionaryId));
                Assert.Equal(0, key.CompareEncodedWith(ref MemoryMarshal.GetReference(dict3EncodedKey), dict3LengthInBits, dict3.DictionaryId));
                Assert.Equal(0, key.CompareEncodedWith(ref MemoryMarshal.GetReference(dict4EncodedKey), dict4LengthInBits, dict4.DictionaryId));
                Assert.Equal(0, key.CompareEncodedWith(ref MemoryMarshal.GetReference(dict5EncodedKey), dict5LengthInBits, dict5.DictionaryId));

                Assert.Equal(stringChars[..j], key.Decoded());

                using var key1 = new CompactKey();
                key1.Initialize(wtx.LowLevelTransaction);
                key1.Set(dict1LengthInBits, dict1EncodedKey, dict1);

                using var key2 = new CompactKey();
                key2.Initialize(wtx.LowLevelTransaction);
                key2.Set(dict2LengthInBits, dict2EncodedKey, dict2.DictionaryId);

                using var key3 = new CompactKey();
                key3.Initialize(wtx.LowLevelTransaction);
                key3.Set(dict3LengthInBits, dict3EncodedKey, dict3.DictionaryId);

                using var key4 = new CompactKey();
                key4.Initialize(wtx.LowLevelTransaction);
                key4.Set(dict4LengthInBits, dict4EncodedKey, dict4.DictionaryId);

                Assert.Equal(0, key.Compare(key1));
                Assert.Equal(0, key.Compare(key2));
                Assert.Equal(0, key.Compare(key3));
                Assert.Equal(0, key.Compare(key4));

                j++;
            }
        }
    }
}
