// -----------------------------------------------------------------------
//  <copyright file="Iterating.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Xunit;
using Voron;
using Voron.Data;

namespace FastTests.Voron.Bugs
{
    public class Iterating : StorageTest
    {
        [Fact]
        public void IterationShouldNotFindAnyRecordsAndShouldNotThrowWhenNumberOfEntriesOnPageIs1AndKeyDoesNotMatch()
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
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
                using (var iterator = snapshot.ReadTree("tree").Iterate())
                {
                    Assert.False(iterator.Seek(Slice.From(snapshot.Allocator, @"Raven\Filesystem\")));
                }
            }
        }
    }
}