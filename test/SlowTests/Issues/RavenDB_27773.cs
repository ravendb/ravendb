using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27773(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public void ReusedPageLocatorCachesReadOnlyPageAccessedInPreviousTransaction()
    {
        var rootPage = CreateTreeAndGetRootPage();
        var persistentContext = new TransactionPersistentContext();

        PageLocator locator;
        using (var tx = Env.ReadTransaction(persistentContext))
        {
            locator = tx.LowLevelTransaction.PageLocator;
            tx.LowLevelTransaction.GetPage(rootPage);
        }

        using (var tx = Env.ReadTransaction(persistentContext))
        {
            Assert.Same(locator, tx.LowLevelTransaction.PageLocator);
            tx.LowLevelTransaction.GetPage(rootPage);
            Assert.True(tx.LowLevelTransaction.PageLocator.TryGetReadOnlyPage(rootPage, out _));
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ReusedPageLocatorCachesWritablePageModifiedInPreviousTransaction()
    {
        var rootPage = CreateTreeAndGetRootPage();
        var persistentContext = new TransactionPersistentContext();

        PageLocator locator;
        using (var tx = Env.WriteTransaction(persistentContext))
        {
            locator = tx.LowLevelTransaction.PageLocator;
            tx.LowLevelTransaction.ModifyPage(rootPage);
        }

        using (var tx = Env.WriteTransaction(persistentContext))
        {
            Assert.Same(locator, tx.LowLevelTransaction.PageLocator);
            tx.LowLevelTransaction.ModifyPage(rootPage);
            Assert.True(tx.LowLevelTransaction.PageLocator.TryGetWritablePage(rootPage, out _));
        }
    }

    private long CreateTreeAndGetRootPage()
    {
        using (var tx = Env.WriteTransaction())
        {
            var tree = tx.CreateTree("test");
            tree.Add("key", "value");
            tx.Commit();
        }

        using (var tx = Env.ReadTransaction())
            return tx.ReadTree("test").State.Header.RootPageNumber;
    }
}
