using System;
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
        var entries = new IndexWriter.EntriesModifications(bsc);
        
        entries.Addition(2);
        entries.Removal(1);
        entries.Addition(3);
        entries.Removal(2);
        entries.PrepareDataForCommiting();

        AssertEntriesCase(ref entries);
    }
    private static void AssertEntriesCase(ref IndexWriter.EntriesModifications entries)
    {
        var additions = entries.Additions;
        var removals = entries.Removals;

        foreach (var add in additions)
            Assert.True(0 > removals.BinarySearch(add));

        foreach (var removal in removals)
            Assert.True(0 > additions.BinarySearch(removal));
    }
}
