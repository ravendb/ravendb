using FastTests.Voron;
using Tests.Infrastructure;
using Voron.Data.BTrees;
using Xunit;

namespace SlowTests.Voron.Issues;

public class RavenDB_27291(ITestOutputHelper output) : StorageTest(output)
{
    private const string TreeName = "reduce-tree";

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Compression)]
    public void DeletingTheLastEntryOfASaturatedCompressedPageMustNotCorruptTheTree()
    {
        using (var tx = Env.WriteTransaction())
        {
            // the shape MapReduceResultsStore builds for a reduce tree on 64 bits
            tx.CreateTree(TreeName, flags: TreeFlags.LeafsCompressed);
            tx.Commit();
        }

        for (int batch = 0; batch < 8_250; batch += 250)
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree(TreeName);

                for (int id = batch; id < batch + 250; id++)
                {
                    var value = new byte[250 + id % 41 * 11];
                    for (int i = 0; i < value.Length; i++)
                        value[i] = (byte)('a' + (id + i / 16) % 26);

                    tree.Add(Key(id), value);
                }

                tx.Commit();
            }
        }

        using (var tx = Env.ReadTransaction())
            Assert.Equal(3, tx.ReadTree(TreeName).ReadHeader().Depth);

        using (var tx = Env.WriteTransaction())
        {
            var tree = tx.ReadTree(TreeName);

            for (int id = 8; id < 115; id++)
                tree.Delete(Key(id));

            for (int i = 0; i < 32; i++)
            {
                tree.Add(Key(i % 8) + "#" + i / 8, new byte[1]);
                tree.Delete(Key(i % 8) + "#" + i / 8);
            }

            for (int id = 1; id < 8; id++)
                tree.Delete(Key(id));

            tree.Delete(Key(0));

            tx.Commit();
        }

        using (var tx = Env.ReadTransaction())
        {
            var tree = tx.ReadTree(TreeName);
            tree.ValidateTree_Forced(tree.ReadHeader().RootPageNumber);
        }
        
        string Key(int id) => $"{id:D8}{new string('x', 92)}";
    }
}
