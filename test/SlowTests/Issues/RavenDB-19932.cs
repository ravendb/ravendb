using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19932 : RavenTestBase
{
    public RavenDB_19932(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void GrowableHashSetForProjectionInCoraxIndexReadOperation()
    {
        var growableHashSet = new CoraxIndexReadOperation.GrowableHashSet<ulong>(maxSizePerCollection: 100);
        var random = new Random(64352);
        var hashset = new HashSet<ulong>();

        for (int i = 0; i < 10_000; ++i)
        {
            var rand = (ulong)random.NextInt64(0, long.MaxValue);
            Assert.Equal(hashset.Add(rand), growableHashSet.Add(rand));
        }

        Assert.True(growableHashSet.HasMultipleHashSets);
        var shuffledList = hashset.ToList();
        shuffledList.Shuffle();

        for (int i = 0; i < shuffledList.Count; ++i)
        {
            Assert.True(growableHashSet.Contains(shuffledList[i])); //exists
            Assert.False(growableHashSet.Add(shuffledList[i])); //cannot add again
        }
    }
}
