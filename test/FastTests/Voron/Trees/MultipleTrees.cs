using System;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Voron.Trees
{
    public class MultipleTrees(ITestOutputHelper output) : StorageTest(output)
    {
        [RavenFact(RavenTestCategory.Voron)]
        public void CanCreateNewTree()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("test");

                tx.CreateTree("test").Add("test", StreamFor("abc"));

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                Assert.True(tx.ReadTree("test").TryRead("test", out _));

                tx.Commit();
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CanUpdateValuesInSubTree()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("test");

                tx.CreateTree("test").Add("test", StreamFor("abc"));

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {

                tx.CreateTree("test").Add("test2", StreamFor("abc"));

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                Assert.True(tx.CreateTree("test").TryRead("test2", out _));
                tx.Commit();
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CreatingTreeWithoutCommitingTransactionShouldYieldNoResults()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("test");
            }

            var e = Assert.Throws<InvalidOperationException>(() =>
                {
                    using (var tx = Env.ReadTransaction())
                    {
                        tx.CreateTree("test");
                    }
                });
            Assert.Contains("No such tree: 'test'", e.Message);
        }
    }
}
