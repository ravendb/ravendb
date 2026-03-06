using Corax.Indexing;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

public class EntriesModificationsTests : NoDisposalNeeded
{
    public EntriesModificationsTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void EntriesModificationsWillEraseOddDuplicates()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        var entries = new EntriesModifications(0);
        
        entries.Addition(bsc, new DocumentEntryId(2), -1, 1, InserterMode.ExactInsert);
        entries.Removal(bsc, new DocumentEntryId(1), -1,1, InserterMode.ExactInsert);
        entries.Addition(bsc, new DocumentEntryId(3), -1,1, InserterMode.ExactInsert);
        entries.Removal(bsc, new DocumentEntryId(2), -1,1, InserterMode.ExactInsert);
        entries.Prepare(bsc);

        AssertEntriesCase(ref entries);
        Assert.Equal(1, entries.Updates.Count);
        Assert.Equal(new DocumentEntryId(2), entries.Updates.ToSpan()[0].EntryId);
    }
    private static void AssertEntriesCase(ref EntriesModifications entries)
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
