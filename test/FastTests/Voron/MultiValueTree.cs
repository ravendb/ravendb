using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using FastTests.Voron.FixedSize;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Voron.Data;
using Xunit;

namespace FastTests.Voron
{
    public class MultiValueTree(ITestOutputHelper output) : StorageTest(output)
    {
        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(false)]
        [InlineData(true)]
        public void Single_MultiAdd_And_Read_DataStored(bool multiBulkAdd)
        {
            var random = new Random();
            var buffer = new byte[1000];
            random.NextBytes(buffer);

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("foo");

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                using var _ = Slice.From(Allocator, buffer, out var key);
                if (multiBulkAdd)
                {
                    Span<Slice> values = stackalloc Slice[1];
                    values[0] = key;
                    using var __ = Slice.From(Allocator, "ChildTreeKey", out var keyTree);
                    tx.ReadTree("foo").MultiBulkAdd(keyTree, values);
                }
                else
                    tx.ReadTree("foo").MultiAdd("ChildTreeKey", key);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                using (var fetchedDataIterator = tx.ReadTree("foo").MultiRead("ChildTreeKey"))
                {
                    fetchedDataIterator.Seek(Slices.BeforeAllKeys);
                    using (Slice.From(Allocator, buffer, out var key))
                    {
                        Assert.True(SliceComparer.Equals(fetchedDataIterator.CurrentKey, key));
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(true)]
        [InlineData(false)]
        public void MultiDelete_Remains_One_Entry_The_Data_Is_Retrieved_With_MultiRead(bool multiBulkAdd)
        {
            const int INPUT_COUNT = 3;
            const int INPUT_DATA_SIZE = 1000;
            const string CHILDTREE_KEY = "ChildTree";

            var inputData = new List<string>();
            for (int i = 0; i < INPUT_COUNT; i++)
            {
                inputData.Add(RandomString(INPUT_DATA_SIZE));
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                if (multiBulkAdd)
                {
                    using var _ = Slice.From(Allocator, CHILDTREE_KEY, out var keyTree);
                    var values = ListToSortedSliceList(tx.Allocator, inputData);
                    tree.MultiBulkAdd(keyTree, values);
                }
                else
                {
                    for (int i = 0; i < INPUT_COUNT; i++)
                    {
                        tree.MultiAdd(CHILDTREE_KEY, inputData[i]);
                    }
                }

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < INPUT_COUNT - 1; i++)
                {
                    tree.MultiDelete(CHILDTREE_KEY, inputData[i]);
                    inputData.Remove(inputData[i]);
                }

                tx.Commit();
            }

            ValidateInputExistence(inputData, CHILDTREE_KEY, INPUT_DATA_SIZE, "foo");
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(true)]
        [InlineData(false)]
        public void MultiDelete_Remains_No_Entries_ChildTreeKey_Doesnt_Exist(bool multiBulkAdd)
        {
            const int INPUT_COUNT = 3;
            const int INPUT_DATA_SIZE = 1000;
            const string CHILDTREE_KEY = "ChildTree";

            var inputData = new List<string>();
            for (int i = 0; i < INPUT_COUNT; i++)
            {
                inputData.Add(RandomString(INPUT_DATA_SIZE));
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");

                if (multiBulkAdd)
                {
                    using var _ = Slice.From(Allocator, CHILDTREE_KEY, out var keyTree);
                    var values = ListToSortedSliceList(tx.Allocator, inputData);
                    tree.MultiBulkAdd(keyTree, values);
                }
                else
                {
                    for (int i = 0; i < INPUT_COUNT; i++)
                    {
                        tree.MultiAdd(CHILDTREE_KEY, inputData[i]);
                    }
                }


                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < INPUT_COUNT; i++)
                {
                    tree.MultiDelete(CHILDTREE_KEY, inputData[i]);
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var iterator = tx.ReadTree("foo").MultiRead(CHILDTREE_KEY);
                iterator.Seek(Slices.BeforeAllKeys);
                Assert.False(iterator.MoveNext());
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Single_MultiAdd_And_Single_MultiDelete_DataDeleted()
        {
            var random = new Random();
            var buffer = new byte[1000];
            random.NextBytes(buffer);

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("foo");
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                Slice key;
                Slice.From(Allocator, buffer, out key);
                tx.CreateTree("foo").MultiAdd("ChildTreeKey", key);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                Slice key;
                Slice.From(Allocator, buffer, out key);
                tx.CreateTree("foo").MultiDelete("ChildTreeKey", key);
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                Assert.Equal(typeof(EmptyIterator), tx.ReadTree("foo").MultiRead("ChildTreeKey").GetType());
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(true)]
        [InlineData(false)]
        public void Multiple_MultiAdd_And_MultiDelete_InTheSame_Transaction_EntryDeleted(bool multiBulkAdd)
        {
            const int INPUT_COUNT = 25;
            const int INPUT_DATA_SIZE = 1000;
            const string CHILDTREE_KEY = "ChildTree";

            var inputData = new List<string>();
            for (int i = 0; i < INPUT_COUNT; i++)
            {
                inputData.Add(RandomString(INPUT_DATA_SIZE));
            }

            var indexToDelete = new Random(1234).Next(0, INPUT_COUNT - 1);
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                if (multiBulkAdd)
                {
                    using var _ = Slice.From(Allocator, CHILDTREE_KEY, out var keyTree);
                    var values = ListToSortedSliceList(tx.Allocator, inputData);
                    tree.MultiBulkAdd(keyTree, values);
                }
                else
                {
                    for (int i = 0; i < INPUT_COUNT; i++)
                    {
                        tree.MultiAdd(CHILDTREE_KEY, inputData[i]);
                    }
                }

                tree.MultiDelete(CHILDTREE_KEY, inputData[indexToDelete]);
                tx.Commit();
            }

            inputData.Remove(inputData[indexToDelete]);
            ValidateInputExistence(inputData, CHILDTREE_KEY, INPUT_DATA_SIZE, "foo");
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(true)]
        [InlineData(false)]
        public void NamedTree_Multiple_MultiAdd_And_MultiDelete_InTheSame_Transaction_EntryDeleted(bool multiBulkAdd)
        {
            const int INPUT_COUNT = 25;
            const int INPUT_DATA_SIZE = 1000;
            const string CHILDTREE_KEY = "ChildTree";

            var inputData = new List<string>();
            for (int i = 0; i < INPUT_COUNT; i++)
            {
                inputData.Add(RandomString(INPUT_DATA_SIZE));
            }

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("foo");
                tx.Commit();
            }

            var indexToDelete = new Random(1234).Next(0, INPUT_COUNT - 1);
            using (var tx = Env.WriteTransaction())
            {
                if (multiBulkAdd)
                {
                    using var _ = Slice.From(Allocator, CHILDTREE_KEY, out var keyTree);
                    var values = ListToSortedSliceList(tx.Allocator, inputData);
                    tx.CreateTree("foo").MultiBulkAdd(keyTree, values);
                }
                else
                {
                    for (int i = 0; i < INPUT_COUNT; i++)
                    {
                        tx.CreateTree("foo").MultiAdd(CHILDTREE_KEY, inputData[i]);
                    }
                }

                tx.CreateTree("foo").MultiDelete(CHILDTREE_KEY, inputData[indexToDelete]);
                tx.Commit();
            }

            inputData.Remove(inputData[indexToDelete]);

            ValidateInputExistence(inputData, CHILDTREE_KEY, INPUT_DATA_SIZE, "foo");
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(true)]
        [InlineData(false)]
        public void NamedTree_Multiple_MultiAdd_MultiDelete_Once_And_Read_EntryDeleted(bool multiBulkAdd)
        {
            const int INPUT_COUNT = 25;
            const int INPUT_DATA_SIZE = 1000;
            const string CHILDTREE_KEY = "ChildTree";

            var inputData = new List<string>();
            for (int i = 0; i < INPUT_COUNT; i++)
            {
                inputData.Add(RandomString(INPUT_DATA_SIZE));
            }

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("foo");
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                if (multiBulkAdd)
                {
                    using var _ = Slice.From(Allocator, CHILDTREE_KEY, out var keyTree);
                    var values = ListToSortedSliceList(tx.Allocator, inputData);
                    tx.CreateTree("foo").MultiBulkAdd(keyTree, values);
                }
                else
                {
                    for (int i = 0; i < INPUT_COUNT; i++)
                    {
                        tx.CreateTree("foo").MultiAdd(CHILDTREE_KEY, inputData[i]);
                    }
                }

                tx.Commit();
            }

            var indexToDelete = new Random(1234).Next(0, INPUT_COUNT - 1);

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("foo").MultiDelete(CHILDTREE_KEY, inputData[indexToDelete]);
                tx.Commit();
            }

            inputData.Remove(inputData[indexToDelete]);

            ValidateInputExistence(inputData, CHILDTREE_KEY, INPUT_DATA_SIZE, "foo");
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void MultiAdd_Twice_TheSame_KeyValue_MultiDelete_NotThrowsException_MultiTree_Deleted()
        {
            const string CHILDTREE_KEY = "ChildTree";
            const string CHILDTREE_VALUE = "Foo";
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.MultiAdd(CHILDTREE_KEY, CHILDTREE_VALUE);
                tree.MultiAdd(CHILDTREE_KEY, CHILDTREE_VALUE);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.MultiDelete(CHILDTREE_KEY, CHILDTREE_VALUE);
                tx.Commit();
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(true)]
        [InlineData(false)]
        public void Multiple_MultiAdd_MultiDelete_Once_And_Read_EntryDeleted(bool multiBulkAdd)
        {
            const int INPUT_COUNT = 25;
            const int INPUT_DATA_SIZE = 1000;
            const string CHILDTREE_KEY = "ChildTree";

            var inputData = new List<string>();
            for (int i = 0; i < INPUT_COUNT; i++)
            {
                inputData.Add(RandomString(INPUT_DATA_SIZE));
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                if (multiBulkAdd)
                {
                    using var _ = Slice.From(Allocator, CHILDTREE_KEY, out var keyTree);
                    var values = ListToSortedSliceList(tx.Allocator, inputData);
                    tree.MultiBulkAdd(keyTree, values);
                }
                else
                {
                    for (int i = 0; i < INPUT_COUNT; i++)
                    {
                        tree.MultiAdd(CHILDTREE_KEY, inputData[i]);
                    }
                }

                tx.Commit();
            }

            ValidateInputExistence(inputData.ToList(), CHILDTREE_KEY, INPUT_DATA_SIZE, "foo");

            var indexToDelete = new Random(1234).Next(0, INPUT_COUNT - 1);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.MultiDelete(CHILDTREE_KEY, inputData[indexToDelete]);
                tx.Commit();
            }

            inputData.Remove(inputData[indexToDelete]);

            ValidateInputExistence(inputData, CHILDTREE_KEY, INPUT_DATA_SIZE, "foo");
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(true)]
        [InlineData(false)]
        public void Multiple_MultiAdd_And_Read_DataStored(bool multiBulkAdd)
        {
            const int INPUT_COUNT = 3;
            const int INPUT_DATA_SIZE = 1000;
            const string CHILDTREE_KEY = "ChildTree";

            var inputData = new List<string>();
            for (int i = 0; i < INPUT_COUNT; i++)
            {
                inputData.Add(RandomString(INPUT_DATA_SIZE));
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                if (multiBulkAdd)
                {
                    using var _ = Slice.From(Allocator, CHILDTREE_KEY, out var keyTree);
                    var values = ListToSortedSliceList(tx.Allocator, inputData);
                    tree.MultiBulkAdd(keyTree, values);
                }
                else
                {
                    for (int i = 0; i < INPUT_COUNT; i++)
                    {
                        tree.MultiAdd(CHILDTREE_KEY, inputData[i]);
                    }
                }

                tx.Commit();
            }

            ValidateInputExistence(inputData, CHILDTREE_KEY, INPUT_DATA_SIZE, "foo");
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineDataWithRandomSeed]
        public void MultiBulkAddFuzzy(int seed)
        {
            var random = new Random(seed);
            var numberOfKeys = random.Next(16, 2000);
            var inMemoryDatabase = new Dictionary<string, HashSet<string>>();
            for (int i = 0; i < numberOfKeys; i++)
            {
                var valuesCount = random.Next(1, 100);
                var key = RandomString(random.Next(4, 32));
                ref var values = ref CollectionsMarshal.GetValueRefOrAddDefault(inMemoryDatabase, key, out var exists);
                if (exists)
                {
                    i--;
                    continue;
                }

                values ??= new();
                do
                {
                    var randomStr = RandomString(random.Next(4, 32));
                    values.Add(randomStr);
                } while (values.Count < valuesCount);
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree(nameof(MultiBulkAddFuzzy));
                foreach (var (key, values) in inMemoryDatabase)
                {
                    using var _ = Slice.From(Allocator, key, out var keyTree);
                    var valuesSlice = ListToSortedSliceList(tx.Allocator, values);
                    tree.MultiBulkAdd(keyTree, valuesSlice);
                }

                tx.Commit();
            }

            ValidateWithInMemoryDatabase();

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree(nameof(MultiBulkAddFuzzy));
                foreach (var (key, values) in inMemoryDatabase)
                {
                    using var _ = Slice.From(Allocator, key, out var keyTree);
                    var valuesSlice = ListToSortedSliceList(tx.Allocator, values);
                    tree.MultiBulkAdd(keyTree, valuesSlice);
                }

                tx.Commit();
            }

            ValidateWithInMemoryDatabase();

            {
                Dictionary<string, HashSet<string>> toRemove = new();
                foreach (var (key, values) in inMemoryDatabase)
                {
                    if (random.Next() % 2 == 0)
                        continue;

                    var valuesToRemove = new HashSet<string>();
                    toRemove.Add(key, valuesToRemove);

                    foreach (var value in values)
                    {
                        if (random.Next() % 9 == 0)
                            continue;

                        valuesToRemove.Add(value);
                    }
                }


                using (var wTx = Env.WriteTransaction())
                {
                    var tree = wTx.CreateTree(nameof(MultiBulkAddFuzzy));
                    foreach (var (key, valuesToRemove) in toRemove)
                    {
                        foreach (var valueToRemove in valuesToRemove)
                        {
                            inMemoryDatabase[key].Remove(valueToRemove);
                            tree.MultiDelete(key, valueToRemove);
                        }
                    }

                    wTx.Commit();
                }
            }

            ValidateWithInMemoryDatabase();

            void ValidateWithInMemoryDatabase()
            {
                using var rTx = Env.ReadTransaction();
                var tree = rTx.CreateTree(nameof(MultiBulkAddFuzzy));
                //Assert.True(tree.IsMultiValueTree);
                foreach (var (key, inMemoryValues) in inMemoryDatabase)
                {
                    using var iterator = tree.MultiRead(key);
                    List<string> valuesList = new();
                    var exists = iterator.Seek(Slices.BeforeAllKeys);
                    Assert.Equal(inMemoryValues.Count > 0, exists);

                    if (exists == false)
                        continue;

                    do
                    {
                        var value = iterator.CurrentKey.ToString();
                        valuesList.Add(value);
                    } while (iterator.MoveNext());

                    Assert.Equal(inMemoryValues.Count, valuesList.Count);
                    foreach (var value in inMemoryValues)
                        Assert.Contains(value, valuesList);
                }
            }
        }
        
        private void ValidateInputExistence(List<string> inputData, string childtreeKey, int inputDataSize, string treeName)
        {
            using (var tx = Env.ReadTransaction())
            {
                var targetTree = tx.ReadTree(treeName);

                int fetchedEntryCount = 0;
                var inputEntryCount = inputData.Count;
                using (var fetchedDataIterator = targetTree.MultiRead(childtreeKey))
                {
                    fetchedDataIterator.Seek(Slices.BeforeAllKeys);
                    do
                    {
                        Assert.Equal(inputDataSize, fetchedDataIterator.CurrentKey.Size);

                        var value = fetchedDataIterator.CurrentKey.ToString();
                        Assert.True(inputData.Contains(value));
                        inputData.Remove(value);
                        fetchedEntryCount++;
                    } while (fetchedDataIterator.MoveNext());

                    Assert.Equal(inputEntryCount, fetchedEntryCount);
                    Assert.Empty(inputData);
                }
            }
        }

        private readonly Random _rng = new Random(123746);
        private const string Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

        private string RandomString(int size)
        {
            var buffer = new char[size];

            for (int i = 0; i < size; i++)
            {
                buffer[i] = Chars[_rng.Next(Chars.Length)];
            }

            return new string(buffer);
        }

        /// <summary>
        /// This leaks memory, but it's supposed to be in short-lived transaction, so it will be disposed with transaction anyway. 
        /// </summary>
        private static ReadOnlySpan<Slice> ListToSortedSliceList<TData>(ByteStringContext context, TData inputData)
            where TData : IEnumerable<string>
        {
            List<Slice> sortedData = new List<Slice>();
            foreach (var inputDataItem in inputData.Distinct())
            {
                Slice.From(context, inputDataItem, ByteStringType.Immutable, out Slice str);
                sortedData.Add(str);
            }

            var span = CollectionsMarshal.AsSpan(sortedData);
            span.Sort(SliceComparer.Instance);
            return span;
        }
    }
}
