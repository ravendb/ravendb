using System;
using System.IO;
using System.Linq;
using Raven.Server.Utils;
using Sparrow.Server.Platform;
using Tests.Infrastructure;
using Voron;
using Voron.Exceptions;
using Voron.Impl.FileHeaders;
using Voron.Schema.Updates;
using Xunit;

namespace FastTests.Voron.Bugs;

public class MetadataJournalId(ITestOutputHelper output) : StorageTest(output)
{
    protected override void Configure(StorageEnvironmentOptions options)
    {
        options.ManualFlushing = true; // committed data stays journal-only, as after a crash
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CommittedDataSurvivesLosingMetadataFile()
    {
        RequireFileBasedPager();

        using (var tx = Env.WriteTransaction())
        {
            tx.CreateTree("t").Add("k", "marker");
            tx.Commit();
        }

        var originalId = Env.HeaderAccessor.MetadataAccessor.JournalId;

        StopDatabase(shouldDisposeOptions: true);

        File.Delete(Path.Combine(DataDir, MetadataAccessor.MetadataName));

        Options = StorageEnvironmentOptions.ForPathForTests(DataDir);
        Configure(Options);
        StartDatabase();

        using (var tx = Env.ReadTransaction())
        {
            Assert.NotNull(tx.ReadTree("t").Read("k"));
        }

        // the id was re-adopted from the journals, not minted fresh
        Assert.Equal(originalId, Env.HeaderAccessor.MetadataAccessor.JournalId);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ForeignJournalMakesRebuiltIdUnresolvable()
    {
        RequireFileBasedPager();

        using (var tx = Env.WriteTransaction())
        {
            tx.CreateTree("t").Add("k", "marker");
            tx.Commit();
        }

        // a second environment supplies a journal stamped with a different id
        var foreignDir = RavenTestHelper.NewDataPath(nameof(MetadataJournalId), 0);
        var foreignOptions = StorageEnvironmentOptions.ForPathForTests(foreignDir);
        foreignOptions.ManualFlushing = true;
        using (var foreign = new StorageEnvironment(foreignOptions))
        using (var tx = foreign.WriteTransaction())
        {
            tx.CreateTree("f").Add("k", "marker");
            tx.Commit();
        }

        StopDatabase(shouldDisposeOptions: true);

        var journalsDir = Path.Combine(DataDir, "Journals");
        var nextNumber = Directory.GetFiles(journalsDir, "*.journal").Length;
        File.Copy(
            Directory.GetFiles(Path.Combine(foreignDir, "Journals"), "*.journal").Single(),
            Path.Combine(journalsDir, StorageEnvironmentOptions.JournalName(nextNumber)));

        File.Delete(Path.Combine(DataDir, MetadataAccessor.MetadataName));

        Options = StorageEnvironmentOptions.ForPathForTests(DataDir);
        Configure(Options);

        // Two candidate ids. Adopting either could replay a foreign environment's
        // transactions. The open must refuse instead of guessing.
        var e = Assert.Throws<VoronUnrecoverableErrorException>(() => StartDatabase());
        Assert.Contains("distinct journal ids", e.Message);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void LoneIdInHardLinkedJournalIsNotAdopted()
    {
        RequireFileBasedPager();

        using (var tx = Env.WriteTransaction())
        {
            tx.CreateTree("t").Add("k", "marker");
            tx.Commit();
        }

        var foreignDir = RavenTestHelper.NewDataPath(nameof(MetadataJournalId), 0);
        var foreignOptions = StorageEnvironmentOptions.ForPathForTests(foreignDir);
        foreignOptions.ManualFlushing = true;
        using (var foreign = new StorageEnvironment(foreignOptions))
        using (var tx = foreign.WriteTransaction())
        {
            tx.CreateTree("f").Add("k", "marker");
            tx.Commit();
        }

        StopDatabase(shouldDisposeOptions: true);

        // The hard-linked foreign journal becomes the only journal. The scan then sees a
        // lone foreign id. Only the link evidence separates this from a legitimate self-heal.
        var journalsDir = Path.Combine(DataDir, "Journals");
        foreach (var journal in Directory.GetFiles(journalsDir, "*.journal"))
            File.Delete(journal);

        var rc = Pal.rvn_ensure_hard_link_non_durable(
            Directory.GetFiles(Path.Combine(foreignDir, "Journals"), "*.journal").Single(),
            Path.Combine(journalsDir, StorageEnvironmentOptions.JournalName(0)),
            out var errorCode);
        Assert.Equal(PalFlags.FailCodes.Success, rc);

        File.Delete(Path.Combine(DataDir, MetadataAccessor.MetadataName));

        Options = StorageEnvironmentOptions.ForPathForTests(DataDir);
        Configure(Options);

        var e = Assert.Throws<VoronUnrecoverableErrorException>(() => StartDatabase());
        Assert.Contains("shared-journal involvement", e.Message);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void UpgradeFrom24PreservesJournalId()
    {
        RequireFileBasedPager();

        var before = Env.HeaderAccessor.MetadataAccessor.JournalId;

        new From24().Update(24, Env.Options, Env.HeaderAccessor, out var versionAfterUpgrade);

        Assert.Equal(25, versionAfterUpgrade);
        Assert.Equal(before, Env.HeaderAccessor.MetadataAccessor.JournalId);
    }
}
