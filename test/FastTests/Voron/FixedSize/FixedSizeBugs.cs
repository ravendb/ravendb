using System;
using Voron;
using Xunit;

namespace FastTests.Voron.FixedSize
{
    public class FixedSizeBugs : StorageTest
    {
        [Fact]
        public void CanAddDuplicate()
        {
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor("test", valSize:8);

                fst.Add(1, new byte[8]);
                fst.Add(2, new byte[8]);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor("test", valSize: 8);
                fst.DebugRenderAndShow();
                fst.Add(1, new byte[8]);
                fst.DebugRenderAndShow();
                fst.Add(2, new byte[8]);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor("test", valSize: 8);

                Assert.Equal(2, fst.NumberOfEntries);
                using (var it = fst.Iterate())
                {
                    Assert.True(it.Seek(0));
                    Assert.Equal(1, it.CurrentKey);
                    Assert.True(it.MoveNext());
                    Assert.Equal(2, it.CurrentKey);
                    Assert.False(it.MoveNext());
                }
            }
        }

    }
}