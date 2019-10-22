using System;
using System.IO;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Bugs
{
    public class Deletes : FastTests.Voron.StorageTest
    {
        public Deletes(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void RebalancerIssue()
        {
            const int DocumentCount = 750;

            var rand = new Random();
            var testBuffer = new byte[757];
            rand.NextBytes(testBuffer);


            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree(  "tree1");
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("tree1");
                for (var i = 0; i < DocumentCount; i++)
                    tree.Add("Foo" + i, new MemoryStream(testBuffer));
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("tree1");
                for (var i = 0; i < DocumentCount; i++)
                {
                    if (i >= 180)
                        continue;

                    tree.Delete("Foo" + i);
                }
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var t1 = tx.CreateTree("tree1");
                t1.Delete("Foo180"); // rebalancer fails to move 1st node from one branch to another
            }
        }
    }
}
