using System;
using Voron;
using Voron.Data.BTrees;
using Xunit;

namespace FastTests.Voron.Paging
{
    internal static class PrefetchTestData
    {
        public const int ValueSize = 1000;

        public static string Key(string prefix, int i) => prefix + "/" + i.ToString("D8");

        public static void Write(StorageEnvironment env, string treeName, string prefix, int from, int count, int batchSize = int.MaxValue)
        {
            var value = new byte[ValueSize];
            var end = from + count;
            for (var start = from; start < end; start = (int)Math.Min((long)start + batchSize, end))
            {
                using var tx = env.WriteTransaction();
                var tree = tx.CreateTree(treeName);
                for (var i = start; i < (int)Math.Min(end, (long)start + batchSize); i++)
                {
                    BitConverter.TryWriteBytes(value, i); // the value of an item says which item it is
                    tree.Add(Key(prefix, i), value);
                }

                tx.Commit();
            }
        }

        public static unsafe void Verify(Tree tree, string prefix, int i)
        {
            Assert.True(tree.TryRead(Key(prefix, i), out var reader), "Item " + prefix + "/" + i + " was not found");
            Assert.Equal(ValueSize, reader.Length);
            Assert.Equal(i, *(int*)reader.Base);
        }

        public static void VerifyRange(StorageEnvironment env, string treeName, string prefix, int from, int count)
        {
            using var tx = env.ReadTransaction();
            var tree = tx.ReadTree(treeName);
            for (var i = from; i < from + count; i++)
                Verify(tree, prefix, i);
        }
    }
}
