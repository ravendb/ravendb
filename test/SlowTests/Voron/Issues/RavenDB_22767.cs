using System.IO;
using FastTests.Voron;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues;

public class RavenDB_22767 : StorageTest
{
    public RavenDB_22767(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.Voron)]
    public void MustRemoveFailedTransactionFromActiveTransactions()
    {
        Env.NewTransactionCreated += (tx) =>
        {
            throw new InvalidDataException();
        };

        Assert.Throws<InvalidDataException>(() =>
        {
            using (Env.ReadTransaction())
            {

            }
        });

        var oldestTxId = Env.ActiveTransactions.OldestTransaction;
            
        Assert.Equal(0, oldestTxId);
    }
}
