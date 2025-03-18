using System.IO;
using FastTests.Voron;
using FastTests.Voron.Util;
using Voron;
using Voron.Impl.Compaction;
using Voron.Util.Conversion;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12227 : StorageTest
    {
        public RavenDB_12227(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(8)]
        [InlineData(16)]
        [InlineData(128)]
        [InlineData(1024 * 256)]
        public void Can_compact_fixed_size_tree_stored_inside_variable_size_tree(int count)
        {
            RequireFileBasedPager();

            var bytes = new byte[48];

            Slice.From(Allocator, "main-tree", out Slice mainTreeId);
            Slice.From(Allocator, "fst-tree", out Slice fstTreeIdreeId);

            var smallValue = new byte[] {1, 2, 3};

            var bigValue = new byte[128];

            for (int i = 0; i < 128; i++)
            {
                bigValue[i] = (byte)i;
            }

            using (var tx = Env.WriteTransaction())
            {
                var mainTree = tx.CreateTree(mainTreeId);

                var fst = mainTree.FixedTreeFor(fstTreeIdreeId, valSize: 48);

                for (int i = 0; i < count; i++)
                {
                    EndianBitConverter.Little.CopyBytes(i, bytes, 0);
                    fst.Add(i, bytes);
                    Slice read;
                    using (fst.Read(i, out read))
                    {
                        Assert.True(read.HasValue);
                    }
                }

                mainTree.Add("small", smallValue);
                mainTree.Add("big", bigValue);


                tx.Commit();
            }

            Env.Dispose();

            var compactedData = Path.Combine(DataDir, "Compacted");
            StorageCompaction.Execute(StorageEnvironmentOptions.ForPathForTests(DataDir),
                (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPathForTests(compactedData));

            using (var compacted = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(compactedData)))
            {
                using (var tx = compacted.ReadTransaction())
                {
                    var mainTree = tx.CreateTree(mainTreeId);

                    var fst = mainTree.FixedTreeFor(fstTreeIdreeId, valSize: 48);

                    for (int i = 0; i < count; i++)
                    {
                        Assert.True(fst.Contains(i), $"at {i}");
                        Slice read;
                        using (fst.Read(i, out read))
                        {
                            read.CopyTo(bytes);
                            Assert.Equal(i, EndianBitConverter.Little.ToInt32(bytes, 0));
                        }
                    }

                    var readResult = mainTree.Read("small");
                    Assert.Equal(smallValue, readResult.Reader.AsStream().ReadData());

                    readResult = mainTree.Read("big");
                    Assert.Equal(bigValue, readResult.Reader.AsStream().ReadData());
                }
            }
        }


        [Theory]
        [InlineData(8)]
        [InlineData(1024 * 256)]
        public void MoveNext_should_not_throw_after_iterating_over_all_items(int count)
        {
            var bytes = new byte[48];
            Slice.From(Allocator, "test", out Slice treeId);

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);

                for (int i = 0; i < count; i++)
                {
                    EndianBitConverter.Little.CopyBytes(i, bytes, 0);
                    fst.Add(i, bytes);
                    Slice read;
                    using (fst.Read(i, out read))
                    {
                        Assert.True(read.HasValue);
                    }
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);

                using (var it = fst.Iterate())
                {
                    it.Seek(long.MinValue);

                    while (it.MoveNext())
                    {
                        
                    }

                    Assert.False(it.MoveNext());
                }
            }
        }
    }
}
