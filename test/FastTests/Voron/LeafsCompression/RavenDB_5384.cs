using System;
using System.IO;
using System.Linq;
using Voron;
using Voron.Data.BTrees;
using Xunit;

namespace FastTests.Voron.LeafsCompression
{
    public class RavenDB_5384 : StorageTest
    {
        [Theory]
        [InlineData(26)]
        [InlineData(100)]
        public void Can_compress_leaf_pages_and_read_from_them(int iterationCount)
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("tree");

                tree.State.Flags |= TreeFlags.LeafsCompressed;

                tx.Commit();
            }

            var bytes = new byte[256];
            new Random().NextBytes(bytes);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("tree");

                Assert.True(tree.State.Flags.HasFlag(TreeFlags.LeafsCompressed));

                for (int i = 0; i < iterationCount; i++)
                {
                    tree.Add($"items/{i}", new MemoryStream(bytes));
                }

                var compressedLeafs =
                    tree.AllPages()
                        .Select(x => tree.GetReadOnlyTreePage(x))
                        .Where(p => p.Flags.HasFlag(PageFlags.Compressed))
                        .ToList();

                Assert.NotEmpty(compressedLeafs);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("tree");

                for (int i = 0; i < iterationCount; i++)
                {
                    var readResult = tree.Read($"items/{i}");

                    var result = readResult.Reader.ReadBytes(readResult.Reader.Length).ToArray();

                    Assert.Equal(bytes, result);
                }
            }
        }
    }
}