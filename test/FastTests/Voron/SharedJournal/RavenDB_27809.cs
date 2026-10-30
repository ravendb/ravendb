using System.Collections.Generic;
using Tests.Infrastructure;
using Voron.Impl.Journal;
using Xunit;

namespace FastTests.Voron.SharedJournal;

public class RavenDB_27809(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public void BranchFolderIsSyncedUntilItsLinksAreDurable()
    {
        using var record = new WriteAheadJournal.LinkedJournalsRecord();
        record.RegisterNewJournal("branch", 3);

        List<(string Path, long Journal)> folders = record.GetFoldersToSync(3);
        Assert.Equal([("branch", 3L)], folders);

        // a newer flush state can replace the one above before its sync runs, so it must still get the folder
        Assert.Equal([("branch", 3L)], record.GetFoldersToSync(5));

        record.MarkFoldersSynced(folders);
        Assert.Null(record.GetFoldersToSync(5));

        record.RegisterNewJournal("branch", 6);
        Assert.Null(record.GetFoldersToSync(2));
        Assert.Equal([("branch", 6L)], record.GetFoldersToSync(5));
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void LinkCreatedInTheFirstJournalIsSynced()
    {
        using var record = new WriteAheadJournal.LinkedJournalsRecord();
        record.RegisterNewJournal("branch", 0);

        Assert.Equal([("branch", 0L)], record.GetFoldersToSync(0));
    }
}
