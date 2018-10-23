using System;
using System.IO;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron.Data.BTrees;
using Xunit;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_11643_Voron : StorageTest
    {
        [Fact64Bit]
        public void PageRefValidationOnBranchPagesShouldNotThrow()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree", flags: TreeFlags.LeafsCompressed);

                tx.Commit();
            }

            var random = new Random(1);
            var bytes = new byte[1024 * 8];

            // insert
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("tree");

                Assert.True(tree.State.Flags.HasFlag(TreeFlags.LeafsCompressed));

                for (int i = 0; i < 40_000; i++)
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

                for (int i = 0; i < 40_000; i += 4)
                {
                    tree.Delete(GetKey(i));
                }

                tree.Delete(GetKey(33392)); // will create a compression tombstone that will be important for this case during rebalancing

                for (int i = 25_000; i >= 0; i--)
                {
                    tree.Delete(GetKey(i));
                }

                tree.DebugValidateBranchReferences();

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("tree");

                for (int i = 0; i < 40_000; i++)
                {
                    tree.Delete(GetKey(i));
                }

                foreach (var pageNumber in tree.AllPages())
                {
                    var treePage = tree.GetReadOnlyTreePage(pageNumber);

                    if (treePage.IsCompressed)
                    {
                        using (var decompressed = tree.DecompressPage(treePage, skipCache: true))
                        {
                            Assert.Equal(0, decompressed.NumberOfEntries);

                            tree.RemoveEmptyDecompressedPage(decompressed);
                        }
                    }

                }
            }
        }

        private static string GetKey(int i)
        {
            return $"{i:D19}.{i:D19}.{i:D19}.{i:D19}.{i:D19}";
        }
    }
}
