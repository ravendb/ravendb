using FastTests.Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_9916 : StorageTest
    {
        public RavenDB_9916(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public unsafe void Should_not_corrupt_state_deleting_from_nested_page_right_side()
        {
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor("foo", 512);

                byte[] buffer = new byte[512];
                long index = 0;
                var run = true;
                while (run)
                {
                    fst.Add(index++, buffer);
                    if (fst.Depth < 3)
                        continue;

                    foreach (var page in fst.AllPages())
                    {
                        var rootPage = fst.GetReadOnlyPage(page);
                        if (rootPage.NumberOfEntries <= 2)
                            continue;
                        var rightMost = fst.GetReadOnlyPage(rootPage.GetEntry(rootPage.NumberOfEntries - 1)->PageNumber);
                        if (rightMost.IsBranch == false)
                            break;
                        run = rightMost.NumberOfEntries <= 5;
                        break;
                    }

                }
                var midpoint = index / 2;
                for (int i = 1; i < 10_000; i++)
                {
                    fst.Delete(midpoint + i);
                    fst.Add(index++, buffer);
                }

                for (long i = 0; i < 5_000; i++)
                {
                    fst.Delete(i + midpoint + 12_000);
                }


                tx.Commit();
            }
        }
        [Fact]
        public unsafe void Should_not_corrupt_state_deleting_from_nested_page_left_side()
        {
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor("foo", 512);

                byte[] buffer = new byte[512];
                long index = 0;
                var run = true;
                while (run)
                {
                    fst.Add(index++, buffer);
                    if (fst.Depth < 3)
                        continue;

                    foreach (var page in fst.AllPages())
                    {
                        var rootPage = fst.GetReadOnlyPage(page);
                        if (rootPage.NumberOfEntries <= 2)
                            continue;
                        var rightMost = fst.GetReadOnlyPage(rootPage.GetEntry(rootPage.NumberOfEntries - 1)->PageNumber);
                        if (rightMost.IsBranch == false)
                            break;
                        run = rightMost.NumberOfEntries <= 5;
                        break;
                    }

                }
                var midpoint = index / 2;
                for (int i = 1; i < 10_000; i++)
                {
                    fst.Delete(midpoint + i);
                    fst.Add(index++, buffer);
                }

                for (long i = 0; i < 5_000; i++)
                {
                    fst.Delete(i);
                }

                tx.Commit();
            }
        }
    }
}
