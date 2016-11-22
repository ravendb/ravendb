using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Sparrow;
using Voron;
using Voron.Data.BTrees;
using Xunit;

namespace FastTests.Voron.LeafsCompression
{
    public class RavenDB_5384 : StorageTest
    {
        [Theory]
        [InlineData(26, 256)]
        [InlineData(1024, 256)]
        [InlineData(8192, 512)]
        [InlineData(26, 333)]
        [InlineData(1024, 555)]
        public void Can_compress_leaf_pages_and_read_directly_from_them_after_decompression(int iterationCount, int size)
        {
            //TODO arek - add test fon non sequential writes
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree", flags: TreeFlags.LeafsCompressed);
                
                tx.Commit();
            }

            var bytes = new byte[size];
            new Random().NextBytes(bytes);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("tree");

                Assert.True(tree.State.Flags.HasFlag(TreeFlags.LeafsCompressed));

                for (int i = 0; i < iterationCount; i++)
                {
                    tree.Add($"items/{i:D5}", new MemoryStream(bytes));
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

                for (var i = 0; i < iterationCount; i++)
                {
                    Slice key;
                    byte[] result;

                    using (Slice.From(tx.Allocator, $"items/{i:D5}", ByteStringType.Immutable, out key))
                    {
                        unsafe
                        {
                            TreeNodeHeader* node;
                            Func<Slice, TreeCursor> cursor;
                            var page = tree.FindPageFor(key, out node, out cursor, allowCompressed: true);

                            ReadResult readResult;

                            if (page.IsCompressed)
                            {
                                using (var decompressed = tree.DecompressPage(page))
                                {
                                    var nodeNumber = decompressed.NodePositionFor(tx.LowLevelTransaction, key);
                                    node = decompressed.GetNode(nodeNumber);

                                    readResult = new ReadResult(tree.GetValueReaderFromHeader(node), node->Version);
                                }
                            }
                            else
                            {
                                readResult = new ReadResult(tree.GetValueReaderFromHeader(node), node->Version);
                            }

                            result = readResult.Reader.ReadBytes(readResult.Reader.Length).ToArray();
                        }
                    }
                    
                    Assert.Equal(bytes, result);
                }
            }
        }
    }
}