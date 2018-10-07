using System;
using System.IO;
using FastTests.Voron;
using Voron;
using Voron.Data.BTrees;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10382 : StorageTest
    {
        [Fact]
        public void ShouldWork()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree", flags: TreeFlags.LeafsCompressed);

                tx.Commit();
            }

            var random = new Random(1);
            var bytes = new byte[1024 * 8];

            int treeKeys;
            int treeKeysToDel;

            if (IntPtr.Size == 8)
            {
                treeKeys = 40_000;
                treeKeysToDel = 25_000;
            }
            else
            {
                treeKeys = 15_000;
                treeKeysToDel = 10_000;
            }
            int treeKeysAssumedLeft = treeKeys - treeKeysToDel;
            // insert
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("tree");

                Assert.True(tree.State.Flags.HasFlag(TreeFlags.LeafsCompressed));
                for (int i = 0; i < treeKeys; i++)
                {
                    random.NextBytes(bytes);

                    tree.Add(GetKey(i), new MemoryStream(bytes));
                }

                tx.Commit();
            }

            // delete
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("tree");

                Assert.True(tree.State.Flags.HasFlag(TreeFlags.LeafsCompressed));

                for (int i = 0; i < treeKeysToDel; i++)
                {
                    tree.Delete(GetKey(i));
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("tree");

                Assert.True(tree.State.Flags.HasFlag(TreeFlags.LeafsCompressed));

                using (var it = tree.Iterate(prefetch: false))
                {
                    Assert.True(it.Seek(Slices.BeforeAllKeys));

                    var count = 0;
                    do
                    {
                        var key = it.CurrentKey.ToString();

                        Assert.Equal(GetKey(treeKeysToDel + count), key);

                        count++;
                    } while (it.MoveNext());

                    Assert.Equal(treeKeysAssumedLeft, count);
                }
            }
        }

        private static string GetKey(int i)
        {
            return $"{i:D19}.{i:D19}.{i:D19}.{i:D19}.{i:D19}";
        }
    }
}
