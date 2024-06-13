using FastTests.Voron;
using Tests.Infrastructure;
using Voron.Data.Fixed;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron;

public class RavenDB_21926 : StorageTest
{
    public RavenDB_21926(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void MustNotCommitTreeStateIfItWasNotModified()
    {
        var lastCommitedTx = -1L;

        using (var txw = Env.WriteTransaction())
        {
            FixedSizeTree fst = txw.CreateTree("foo").FixedTreeFor("bar", sizeof(long));

            fst.Add(1, 1L);

            txw.Commit();

            lastCommitedTx = txw.LowLevelTransaction.Id;
        }

        using (var txw = Env.WriteTransaction())
        {
            FixedSizeTree fst = txw.CreateTree("foo").FixedTreeFor("bar", sizeof(long));

            txw.Commit(); // should be no op because nothing was changed in this transaction
        }

        using (var txw = Env.WriteTransaction())
        {
            Assert.Equal(lastCommitedTx + 1, txw.LowLevelTransaction.Id);
        }
    }
}
