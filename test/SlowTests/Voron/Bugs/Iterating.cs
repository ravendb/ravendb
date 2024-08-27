// -----------------------------------------------------------------------
//  <copyright file="Iterating.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Bugs
{
    public class Iterating : FastTests.Voron.StorageTest
    {
        public Iterating(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void IterationShouldNotFindAnyRecordsAndShouldNotThrowWhenNumberOfEntriesOnPageIs1AndKeyDoesNotMatch()
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnlyForTests()))
            {
                using (var tx = env.WriteTransaction())
                {
                    tx.CreateTree("tree");

                    tx.Commit();
                }

                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.ReadTree("tree");
                    tree.Add(@"Raven\Database\1", StreamFor("123"));

                    tx.Commit();
                }

                using (var snapshot = env.ReadTransaction())
                using (var iterator = snapshot.ReadTree("tree").Iterate(false))
                {
                    Slice v;
                    Slice.From(snapshot.Allocator, @"Raven\Filesystem\", out v);
                    Assert.False(iterator.Seek(v));
                }
            }
        }
    }
}
