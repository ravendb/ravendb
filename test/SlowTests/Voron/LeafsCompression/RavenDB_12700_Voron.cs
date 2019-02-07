using System.Collections.Generic;
using System.IO;
using FastTests.Voron;
using Raven.Client.Extensions.Streams;
using Raven.Server.Config.Settings;
using Sparrow;
using Voron;
using Voron.Data.BTrees;
using Xunit;

namespace SlowTests.Voron.LeafsCompression
{
    public class RavenDB_12700_Voron : StorageTest
    {
        [Fact]
        public unsafe void Must_split_compressed_page_if_cannot_compress_back_after_decompression()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("tree", flags: TreeFlags.LeafsCompressed);

                Page modifyPage = tx.LowLevelTransaction.ModifyPage(tree.State.RootPageNumber);

                // it's a special page which after decompression could not be compressed back
                // it contains 2 uncompressed nodes and 201 nodes in compressed part
                // the issue was that after decompression the layout of data on the page was different
                // and we couldn't compress it back to 8KB page

                // let's copy the entire page content but we need to change the page number
                var compressedPagePath = new PathSetting("Voron/LeafsCompression/Data/RavenDB-12700-page-1278-compressed");

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

                modifyPage.PageNumber = tree.State.RootPageNumber; // set the original page number

                var readValues = new List<(Slice, byte[])>();

                Slice? idToDelete = null;

                using (var it = tree.Iterate(false))
                {
                    Assert.True(it.Seek(Slices.BeforeAllKeys));

                    do
                    {
                        var key = it.CurrentKey.Clone(tx.Allocator);

                        var reader = it.CreateReaderForCurrent();

                        var bytes = reader.ReadBytes(reader.Length).ToArray();

                        if (idToDelete == null)
                        {
                            idToDelete = key;
                            continue;
                        }

                        readValues.Add((key, bytes));
                    } while (it.MoveNext());
                }
                
                // this deletion will cause page decompression in order to remove one entry
                // under the cover we'll do the page split because we aren't able to compress the page back
                tree.Delete(idToDelete.Value);

                // let's check that after page split we can still read values

                foreach (var item in readValues)
                {
                    using (var result = tree.ReadDecompressed(item.Item1))
                    {
                        Assert.Equal(item.Item2, result.Reader.AsStream().ReadData());
                    }
                }
            }
        }
    }
}
