using System;
using System.Collections.Generic;
using System.IO;
using FastTests.Voron;
using Raven.Client.Extensions.Streams;
using Raven.Server.Config.Settings;
using Sparrow;
using Sparrow.Binary;
using Voron;
using Voron.Data.BTrees;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.LeafsCompression
{
    public class RavenDB_17660 : StorageTest
    {
        public RavenDB_17660(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public unsafe void Must_invalidate_cached_decompressed_page_with_write_usage_after_page_split()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("tree", flags: TreeFlags.LeafsCompressed);

                Page modifyPage = tx.LowLevelTransaction.ModifyPage(tree.ReadHeader().RootPageNumber);

                // it is a special page which after the below sequence of delete and add operations resulted in splitting and
                // caching the outdated version of the decompressed page in DecompressedPagesCache
                // 
                // that in turn resulted later in "Could not add uncompressed node to decompressed page" exception
                // thrown from Voron.Data.BTrees.Tree.HandleUncompressedNodes() during the last delete

                // let's copy the entire page content but we need to change the page number
                var compressedPagePath = new PathSetting("Voron/LeafsCompression/Data/RavenDB-17660-page-5728-compressed");

                byte[] data;

                using (var file = File.Open(compressedPagePath.FullPath, FileMode.Open))
                {
                    data = file.ReadData();

                    Assert.Equal(file.Length, data.Length);
                }

                fixed (byte* dataPtr = data)
                {
                    Memory.Copy(modifyPage.Pointer, dataPtr, data.Length);
                }

                modifyPage.PageNumber = tree.ReadHeader().RootPageNumber; // set the original page number


                var items = new List<(long, OperationOnTree, int)>()
                {
                    (5037765, OperationOnTree.Delete, -1),
                    (5037769, OperationOnTree.Add, 1645),
                    (5037773, OperationOnTree.Delete, -1),
                    (5037777, OperationOnTree.Delete, -1),
                    (5037782, OperationOnTree.Delete, -1),
                    (5037788, OperationOnTree.Delete, -1),
                    (5037794, OperationOnTree.Delete, -1),
                    (5037800, OperationOnTree.Delete, -1),
                    (5037806, OperationOnTree.Delete, -1),
                    (5037812, OperationOnTree.Delete, -1),
                    (5037818, OperationOnTree.Delete, -1),
                    (5037824, OperationOnTree.Delete, -1),
                    (5037830, OperationOnTree.Delete, -1),
                    (5037836, OperationOnTree.Delete, -1),
                    (5037842, OperationOnTree.Delete, -1),
                    (5037848, OperationOnTree.Delete, -1),
                    (5037859, OperationOnTree.Add, 1653),
                    (5037865, OperationOnTree.Add, 1644),
                };

                var random = new Random(1);

                foreach ((long Key, OperationOnTree Operation, int Size) item in items)
                {
                    var id = Bits.SwapBytes(item.Key);

                    using (Slice.External(tx.Allocator, (byte*)&id, sizeof(long), out var idSlice))
                    {
                        switch (item.Operation)
                        {
                            case OperationOnTree.Delete:
                                tree.Delete(idSlice);
                                break;
                            case OperationOnTree.Add:
                                var bytes = new byte[item.Size];
                                random.NextBytes(bytes);

                                tree.Add(idSlice, bytes);
                                break;
                        }
                    }
                }
            }
        }

        private enum OperationOnTree
        {
            Add,
            Delete
        }
    }
}
