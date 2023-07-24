using System;
using System.Diagnostics;
using Corax;
using Sparrow.Server;
using Sparrow.Threading;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class EntriesModificationsTests : NoDisposalNeeded
{
    public EntriesModificationsTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void EntriesModificationsWillEraseOddDuplicates()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        var entries = new IndexWriter.EntriesModifications(bsc, 0);
        
        entries.Addition(2);
        entries.Removal(1);
        entries.Addition(3);
        entries.Removal(2);
        entries.Prepare();

        AssertEntriesCase(ref entries);
        Assert.Equal(1, entries.Updates.Count);
        Assert.Equal(2, entries.Updates.ToSpan()[0].EntryId);
    }
    private static void AssertEntriesCase(ref IndexWriter.EntriesModifications entries)
    {
        var additions = entries.Additions;
        var removals = entries.Removals;

        foreach (var add in additions.ToSpan())
            Assert.True(0 > removals.ToSpan().BinarySearch(add));

        foreach (var removal in removals.ToSpan())
            Assert.True(0 > additions.ToSpan().BinarySearch(removal));
    }
}
