using System;
using Corax;
using Corax.Indexing;
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
        var entries = new EntriesModifications(0, 0);
        
        entries.Addition(bsc, 2, -1, 1);
        entries.Removal(bsc, 1, -1,1);
        entries.Addition(bsc, 3, -1,1);
        entries.Removal(bsc, 2, -1,1);
        entries.Prepare(bsc);

        AssertEntriesCase(ref entries);
        Assert.Equal(1, entries.Updates.Count);
        Assert.Equal(2, entries.Updates.ToSpan()[0].EntryId);
    }
    private static unsafe void AssertEntriesCase(ref EntriesModifications entries)
    {
        var additions = entries.Additions;
        var removals = entries.Removals;

        foreach (var add in additions.ToSpan())
        {
            bool found = false;
            for (int i = 0; i < removals.Count; i++)
            {
                if (add.EntryId == removals[i].EntryId)
                    found = true;
            }
            Assert.False(found);
        }


        foreach (var removal in removals.ToSpan())
        {
            bool found = false;
            for (int i = 0; i < additions.Count; i++)
            {
                if (removal.EntryId == additions[i].EntryId)
                    found = true;
            }
            Assert.False(found);
        }
    }
}
