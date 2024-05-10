// -----------------------------------------------------------------------
//  <copyright file="Increments.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Storage
{
    public class Increments : FastTests.Voron.StorageTest
    {
        public Increments(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void SimpleIncrementShouldWork()
        {
            CreateTrees(Env, 1, "tree");

            using (var tx = Env.WriteTransaction())
            {
                Assert.Equal(10, tx.CreateTree("tree0").Increment("key/1", 10));

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                Assert.Equal(15, tx.CreateTree("tree0").Increment("key/1", 5));

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                Assert.Equal(12, tx.CreateTree("tree0").Increment("key/1", -3));

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var read = tx.ReadTree("tree0").Read("key/1");

                Assert.NotNull(read);
                Assert.Equal(12, read.Reader.Read<long>());
            }
        }

    
        [Fact]
        public void SimpleIncrementEntriesCountShouldStayCorrectAfterCommit()
        {
            CreateTrees(Env, 1, "tree");

            using (var tx = Env.WriteTransaction())
            {
                Assert.Equal(10, tx.CreateTree("tree0").Increment("key/1", 10));

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                Assert.Equal(1, tx.ReadTree("tree0").State.Header.NumberOfEntries);
            }
        }
    }
}
