using System.Collections.Generic;
using System.Linq;
using Tests.Infrastructure;
using Voron.Data.Tables;
using Xunit;

namespace FastTests.Voron.Tables
{
    public class Allocations : TableStorageTest
    {
        public Allocations(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void NewPageAllocatorMustAccountForAllSections()
        {
            using (var tx = Env.WriteTransaction())
            {
                var parent = tx.CreateTree("parent");
                var allocator = new NewPageAllocator(tx.LowLevelTransaction, parent);
                allocator.Create();

                // right after Create() there is exactly one section, so even a single-section scan sees all of it
                int sectionCapacity = allocator.AllPages().Count;
                Assert.True(sectionCapacity > 0);

                var firstSection = new List<long>();
                for (int i = 0; i < sectionCapacity; i++)
                    firstSection.Add(allocator.AllocateSinglePage(0).PageNumber);

                var secondSection = new List<long>();
                for (int i = 0; i < 20; i++)
                    secondSection.Add(allocator.AllocateSinglePage(0).PageNumber);

                allocator.FreePage(firstSection[5]);
                allocator.FreePage(firstSection[10]);
                allocator.FreePage(firstSection[15]);
                allocator.FreePage(secondSection[3]);
                allocator.FreePage(secondSection[7]);

                var expectedFree = 3 + (sectionCapacity - 20) + 2;

                var allPages = allocator.AllPages();
                Assert.Equal(expectedFree, allPages.Count);
                Assert.Contains(firstSection[5], allPages);
                Assert.Contains(secondSection[3], allPages);

                var report = allocator.GetNumberOfPreAllocatedFreePages();
                Assert.Equal(expectedFree, report.NumberOfFreePages);
                Assert.Equal(2L * sectionCapacity, report.NumberOfOriginallyAllocatedPages);
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Voron, RavenArchitecture.AllX64)]
        public void IndexPagesWillBeNearby_64()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs", 16);

                tx.Commit();
            }
            var largeString = new string('a', 1024);
            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");

                for (int i = 0; i < 2500; i++)
                {
                    SetHelper(docs, "users/" + i, "Users", 1L + i, largeString);
                }

                tx.Commit();
            }


            using (var tx = Env.ReadTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");

                foreach (var index in DocsSchema.Indexes)
                {
                    var tree = docs.GetTree(index.Value);
                    Assert.NotEqual(1, tree.State.Header.Depth);
                    var pages = tree.AllPages();
                    var minPage = pages.Min();
                    var maxPage = pages.Max();
                    Assert.True((maxPage - minPage) < 256);
                }
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Voron, RavenArchitecture.AllX86)]
        public void IndexPagesWillBeNearby_32()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs", 16);

                tx.Commit();
            }
            var largeString = new string('a', 1024);
            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");

                for (int i = 0; i < 250; i++)
                {
                    SetHelper(docs, "users/" + i, "Users", 1L + i, largeString);
                }

                tx.Commit();
            }


            using (var tx = Env.ReadTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");

                foreach (var index in DocsSchema.Indexes)
                {
                    var tree = docs.GetTree(index.Value);
                    Assert.NotEqual(1, tree.State.Header.Depth);
                    var pages = tree.AllPages();
                    var minPage = pages.Min();
                    var maxPage = pages.Max();
                    Assert.True((maxPage - minPage) < 128);
                }
            }
        }
    }
}
