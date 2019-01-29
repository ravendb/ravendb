// -----------------------------------------------------------------------
//  <copyright file="LargeFixedSizeTreeBugs.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Xunit;
using Voron;

namespace FastTests.Voron.FixedSize
{
    public class LargeFixedSizeTreeBugs : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            base.Configure(options);
            options.ManualFlushing = true;
        }

        [Fact]
        public void DeleteRangeShouldModifyPage()
        {
            Slice treeId;
            Slice.From(Allocator, "test", out treeId);
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 128);
                var bytes = new byte[128];
                for (int i = 0; i < 100; i++)
                {
                    fst.Add(i, bytes);
                }

                tx.Commit();
            }


            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 128);
                fst.DeleteRange(20, 70);

                tx.Commit();
            }

            RestartDatabase();

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 128);
                Assert.False(fst.Contains(21));
                tx.Commit();
            }
        }
    }
}
