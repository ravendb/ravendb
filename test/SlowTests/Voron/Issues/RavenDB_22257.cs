using System;
using System.Threading;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Impl;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues;

public class RavenDB_22257 : StorageTest
{
    public RavenDB_22257(ITestOutputHelper output) : base(output)
    {
    }

    protected override void Configure(StorageEnvironmentOptions options)
    {
        base.Configure(options);

        options.ManualFlushing = true;
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void MustNotAllowToHaveTwoWriteTransactionsConcurrently()
    {
        RequireFileBasedPager();

        using (var txw1 = Env.NewLowLevelTransaction(new TransactionPersistentContext(), TransactionFlags.ReadWrite))
        {
            LowLevelTransaction txw2 = null;

            txw1.ModifyPage(0);

            try
            {
                txw1.ForTestingPurposesOnly().CallOnTransactionAfterCommit(() =>
                {
                    /*
                    // here we pretend to be running as of a different thread
                    txw1.Dispose(); // dispose of the already running transaction will release the write transaction lock, so we'll be able to create a new write transaction
                    */

                    InvalidOperationException ex = null;

                    Thread newTransactionThread = new Thread(() =>
                    {
                        ex = Assert.Throws<InvalidOperationException>(() =>
                        {
                            txw1.Dispose(); // this is supposed to throw because we're attempting to dispose write tx from a different thread

                            txw2 = Env.NewLowLevelTransaction(new TransactionPersistentContext(), TransactionFlags.ReadWrite);

                            txw2.ModifyPage(0);
                        });
                    });

                    newTransactionThread.Start();

                    newTransactionThread.Join();

                    Assert.StartsWith("Dispose of the transaction must be called from the same thread that created it. Transaction 2 (Flags: ReadWrite) was created by", ex.Message);
                });

                /* 
                // both commits below will fail BUT they managed to write to the journal file already

                var notFoundInActiveTransactionsEx = Assert.Throws<InvalidOperationException>(txw1.Commit);

                Assert.Equal("The transaction with ID '2' got committed and flushed but it wasn't found in the ActiveTransactions. (Debug details: tx id of ActiveTransactionNode - 2", notFoundInActiveTransactionsEx.Message);

                var duplicatedKeyEx = Assert.Throws<ArgumentException>(txw2.Commit);

                Assert.Equal("An item with the same key has already been added. Key: 2 (Parameter 'key')", duplicatedKeyEx.Message);
                */

                txw1.Commit();

                Assert.Null(txw2);
            }
            finally
            {
                txw2?.Dispose();
            }
        }

        /*
        // during the recovery will encounter two committed transactions with the same ID
        */

        RestartDatabase();

        using (var txw = Env.WriteTransaction())
        {
            txw.CreateTree("foo");
            txw.Commit();
        }
    }
}
