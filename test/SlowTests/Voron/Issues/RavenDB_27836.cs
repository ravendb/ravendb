using FastTests.Voron;
using Sparrow.Platform;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Voron.Issues;

public class RavenDB_27836(ITestOutputHelper output) : StorageTest(output)
{
    private bool _encrypted;

    protected override void Configure(StorageEnvironmentOptions options)
    {
        base.Configure(options);
        if (_encrypted)
            options.Encryption.MasterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());
    }

    [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.Encryption)]
    [InlineData(false)]
    [InlineData(true)]
    public void SkipMustSeeEntriesDeletedEarlierInTheSameWriteTransaction(bool encrypted)
    {
        _encrypted = encrypted;
        RequireFileBasedPager();

        using (var tx = Env.WriteTransaction())
        {
            var fst = tx.FixedTreeFor("f", 8);
            for (long i = 0; i < 500_000; i++)
                fst.Add(i, i);
            var other = tx.FixedTreeFor("g", 8);
            for (long i = 0; i < 1_000_000; i++)
                other.Add(i, i);
            tx.Commit();
        }

        using (var tx = Env.WriteTransaction())
        {
            var fst = tx.FixedTreeFor("f", 8);
            for (long i = 0; i < 500_000; i += 2)
                fst.Delete(i);

            // evicts f's pages from the page locator
            using (var otherIt = tx.FixedTreeFor("g", 8).Iterate())
            {
                Assert.True(otherIt.Seek(0));
                while (otherIt.MoveNext()) { }
            }

            using (var it = fst.Iterate())
            {
                Assert.True(it.Seek(0));
                Assert.True(it.Skip(150_000), "Skip(150000) returned false");
                Assert.Equal(300_001, it.CurrentKey);
            }

            using (var otherIt = tx.FixedTreeFor("g", 8).Iterate())
            {
                Assert.True(otherIt.Seek(0));
                while (otherIt.MoveNext()) { }
            }

            using (var it = fst.Iterate())
            {
                Assert.True(it.SeekToLast());
                Assert.Equal(499_999, it.CurrentKey);
                Assert.True(it.Skip(-150_000), "Skip(-150000) returned false");
                Assert.Equal(199_999, it.CurrentKey);
            }
        }
    }
}
