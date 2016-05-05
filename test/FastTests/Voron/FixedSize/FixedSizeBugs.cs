using Voron;
using Xunit;

namespace FastTests.Voron.FixedSize
{
    public class FixedSizeBugs : StorageTest
    {
        [Fact]
        public void CanAddDuplicate()
        {
            var treeId = Slice.From(Allocator, "test");
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 8);

                fst.Add(2, new byte[8]);
                fst.Add(3, new byte[8]);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 8);
                fst.DebugRenderAndShow();
                fst.Add(1, new byte[8]);
                fst.Add(2, new byte[8]);
                fst.DebugRenderAndShow();

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 8);

                Assert.Equal(3, fst.NumberOfEntries);
                using (var it = fst.Iterate())
                {
                    Assert.True(it.Seek(0));
                    Assert.Equal(1, it.CurrentKey);
                    Assert.True(it.MoveNext());
                    Assert.Equal(2, it.CurrentKey);
                    Assert.True(it.MoveNext());
                    Assert.Equal(3, it.CurrentKey);
                    Assert.False(it.MoveNext());
                }
            }
        }

        [Fact]
        public void CanAddDuplicate_Many()
        {
            var treeId = Slice.From(Allocator, "test");
            using (var tx = Env.WriteTransaction())
            {
                for (var i = 0; i < 300; i++)
                {
                    var fst = tx.FixedTreeFor(treeId, valSize: 8);

                    fst.Add(i, new byte[8]);
                }

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                for (var i = 0; i < 300; i++)
                {
                    var fst = tx.FixedTreeFor(treeId, valSize: 8);

                    fst.Delete(i);
                    fst.Add(i, new byte[8]);
                }

                tx.Commit();
            }
        }
    }
}