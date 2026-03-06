﻿using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Voron.Storage
{
    public class Increments : FastTests.Voron.StorageTest
    {
        public Increments(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Voron)]
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
                Assert.True(tx.ReadTree("tree0").TryRead("key/1", out var reader));
                Assert.Equal(12, reader.ReadLittleEndianInt64());
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
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
