using System;
using System.Collections.Generic;
using System.Linq;
using Voron;
using Voron.Data;
using Xunit;

namespace FastTests.Voron
{
    public class MultiValueTree : StorageTest
    {
        [Fact]
        public void Single_MultiAdd_And_Read_DataStored()
        {
            var random = new Random();
            var buffer = new byte[1000];
            random.NextBytes(buffer);

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree( "foo");

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                tx.ReadTree("foo").MultiAdd("ChildTreeKey", Slice.From(Allocator, buffer));
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                using (var fetchedDataIterator = tx.ReadTree("foo").MultiRead("ChildTreeKey"))
                {
                    fetchedDataIterator.Seek(Slices.BeforeAllKeys);

                    Assert.True(SliceComparer.Equals(fetchedDataIterator.CurrentKey, Slice.From(Allocator, buffer)));
                }
            }
        }

        [Fact]
        public void MultiDelete_Remains_One_Entry_The_Data_Is_Retrieved_With_MultiRead()
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
                for (int i = 0; i < INPUT_COUNT; i++)
                {
                    tree.MultiAdd(CHILDTREE_KEY, inputData[i]);
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

        [Fact]
        public void MultiDelete_Remains_No_Entries_ChildTreeKey_Doesnt_Exist()
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
                for (int i = 0; i < INPUT_COUNT; i++)
                {
                    tree.MultiAdd(CHILDTREE_KEY, inputData[i]);
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

        [Fact]
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
                tx.CreateTree("foo").MultiAdd("ChildTreeKey", Slice.From(Allocator, buffer));
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("foo").MultiDelete("ChildTreeKey", Slice.From(Allocator, buffer));
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                Assert.Equal(typeof(EmptyIterator), tx.ReadTree("foo").MultiRead("ChildTreeKey").GetType());
            }
        }

        [Fact]
        public void Multiple_MultiAdd_And_MultiDelete_InTheSame_Transaction_EntryDeleted()
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
                for (int i = 0; i < INPUT_COUNT; i++)
                {
                    tree.MultiAdd(CHILDTREE_KEY, inputData[i]);
                }

                tree.MultiDelete(CHILDTREE_KEY, inputData[indexToDelete]);
                tx.Commit();
            }

            inputData.Remove(inputData[indexToDelete]);
            ValidateInputExistence(inputData, CHILDTREE_KEY, INPUT_DATA_SIZE, "foo");
        }

        [Fact]
        public void NamedTree_Multiple_MultiAdd_And_MultiDelete_InTheSame_Transaction_EntryDeleted()
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
                for (int i = 0; i < INPUT_COUNT; i++)
                {
                    tx.CreateTree("foo").MultiAdd(CHILDTREE_KEY, inputData[i]);
                }

                tx.CreateTree("foo").MultiDelete(CHILDTREE_KEY, inputData[indexToDelete]);
                tx.Commit();
            }

            inputData.Remove(inputData[indexToDelete]);

            ValidateInputExistence(inputData, CHILDTREE_KEY, INPUT_DATA_SIZE, "foo");
        }

        [Fact]
        public void NamedTree_Multiple_MultiAdd_MultiDelete_Once_And_Read_EntryDeleted()
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
                tx.CreateTree( "foo");
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                for (int i = 0; i < INPUT_COUNT; i++)
                {
                    tx.CreateTree("foo").MultiAdd(CHILDTREE_KEY, inputData[i]);
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

        [Fact]
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

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("foo");
                
                Assert.Equal(0, tree.ReadVersion(CHILDTREE_KEY));
            }
        }
        
        [Fact]
        public void Multiple_MultiAdd_MultiDelete_Once_And_Read_EntryDeleted()
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
                for (int i = 0; i < INPUT_COUNT; i++)
                {
                    tree.MultiAdd(CHILDTREE_KEY, inputData[i]);
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

        [Fact]
        public void Multiple_MultiAdd_And_Read_DataStored()
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

                for (int i = 0; i < INPUT_COUNT; i++)
                {
                    tree.MultiAdd(CHILDTREE_KEY, inputData[i]);
                }
                tx.Commit();
            }

            ValidateInputExistence(inputData, CHILDTREE_KEY, INPUT_DATA_SIZE, "foo");
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

    }
}
