using System.Collections.Generic;
using System.Linq;
using Tests.Infrastructure;
using Voron.Data.Containers;
using Voron.Exceptions;
using Voron.Global;
using Xunit;

namespace FastTests.Voron.Containers;

public class DeferredFreesTests(ITestOutputHelper output) : StorageTest(output)
{
    private const int ItemSize = 100;

    [RavenFact(RavenTestCategory.Voron)]
    public void PendingFreesAreNotReturnedByGetAllIds()
    {
        var (containerId, ids) = CreateContainer(count: 3_000);
        var deleted = TakePerPage(ids, perPage: 1);
        var expected = ids.Except(deleted).Order().ToList();

        using (var wtx = Env.WriteTransaction())
        {
            foreach (long id in deleted)
                Container.DeleteOrDefer(wtx.LowLevelTransaction, containerId, new ContainerEntryId(id));

            Assert.Equal(expected, Container.GetAllIds(wtx.LowLevelTransaction, containerId).Order());
            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            Assert.Equal(deleted.Count, Container.GetNumberOfPendingFrees(rtx.LowLevelTransaction, containerId));
            Assert.Equal(expected, Container.GetAllIds(rtx.LowLevelTransaction, containerId).Order());
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void PendingFreesAreReportedSeparatelyInStorageReport()
    {
        var (containerId, ids) = CreateContainer(count: 3_000);
        long entriesBefore = ReportedEntries("Items (Container)");
        var deleted = TakePerPage(ids, perPage: 1);
        Delete(containerId, deleted);

        Assert.Equal(entriesBefore - deleted.Count, ReportedEntries("Items (Container)"));
        Assert.Equal(deleted.Count, ReportedEntries("Items (Container Pending Frees)"));

        long ReportedEntries(string name)
        {
            using var rtx = Env.ReadTransaction();
            return Env.GenerateDetailedReport(rtx).Trees.Single(x => x.Name == name).NumberOfEntries;
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void AllocationReusesPageWithPendingFrees()
    {
        var (containerId, ids) = CreateContainer(count: 3_000);
        var deleted = TakePerPage(ids, perPage: 5);
        Delete(containerId, deleted);
        var deletedAgain = TakePerPage(ids.Except(deleted).ToList(), perPage: 5);
        Delete(containerId, deletedAgain);
        deleted.AddRange(deletedAgain);

        long pendingBefore;
        using (var rtx = Env.ReadTransaction())
            pendingBefore = Container.GetNumberOfPendingFrees(rtx.LowLevelTransaction, containerId);
        Assert.Equal(deleted.Count, pendingBefore);

        var added = new List<long>();
        using (var wtx = Env.WriteTransaction())
        {
            for (int i = 0; i < 500; i++)
                added.Add((long)Container.Allocate(wtx.LowLevelTransaction, containerId, ItemSize, out _));

            var reused = added.Intersect(deleted).ToList();
            Assert.NotEmpty(reused);
            Assert.Subset(Container.GetAllIds(wtx.LowLevelTransaction, containerId).ToHashSet(), reused.ToHashSet());
            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            Assert.True(Container.GetNumberOfPendingFrees(rtx.LowLevelTransaction, containerId) < pendingBefore);
            Assert.Equal(ids.Except(deleted).Concat(added).Order(), Container.GetAllIds(rtx.LowLevelTransaction, containerId).Order());
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ReclaimScanContinuesInNextTransaction()
    {
        var (containerId, ids) = CreateContainer(count: 3_000);
        var pages = ids.GroupBy(id => id / Constants.Storage.PageSize).ToList();
        Delete(containerId, pages.Skip(1).Take(20).SelectMany(page => page.Take(3)).ToList());
        var reusable = pages[26];
        Delete(containerId, reusable.Take(5).ToList());
        Delete(containerId, reusable.Skip(5).Take(5).ToList());

        long page = AllocateUntilNextPage(containerId, ids.Last() / Constants.Storage.PageSize);
        Assert.NotEqual(reusable.Key, page);
        Assert.Equal(reusable.Key, AllocateUntilNextPage(containerId, page));
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void PendingFreesAreCapped()
    {
        var (containerId, ids) = CreateContainer(count: 60_000);
        var deleted = TakePerPage(ids, perPage: 7);
        Assert.True(deleted.Count > Container.MinPendingCap);
        Delete(containerId, deleted);

        using var rtx = Env.ReadTransaction();
        Assert.InRange(Container.GetNumberOfPendingFrees(rtx.LowLevelTransaction, containerId), 1, Container.MinPendingCap);
        Assert.Equal(ids.Except(deleted).Order(), Container.GetAllIds(rtx.LowLevelTransaction, containerId).Order());
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void DeferringFreeAfterDeferredFreesWereProcessedThrows()
    {
        var (containerId, ids) = CreateContainer(count: 3_000);
        var deleted = TakePerPage(ids, perPage: 1).Take(2).ToList();

        using var wtx = Env.WriteTransaction();
        var llt = wtx.LowLevelTransaction;
        Container.DeleteOrDefer(llt, containerId, new ContainerEntryId(deleted[0]));
        wtx.OnBeforeCommit += _ => Container.DeleteOrDefer(llt, containerId, new ContainerEntryId(deleted[1]));
        Assert.Throws<VoronErrorException>(() => wtx.Commit());
    }

    private (ContainerId ContainerId, List<long> Ids) CreateContainer(int count)
    {
        using var wtx = Env.WriteTransaction();
        var containerId = wtx.OpenContainer("Items");
        var ids = new List<long>();
        for (int i = 0; i < count; i++)
            ids.Add((long)Container.Allocate(wtx.LowLevelTransaction, containerId, ItemSize, out _));
        wtx.Commit();
        return (containerId, ids);
    }

    private void Delete(ContainerId containerId, List<long> ids)
    {
        using var wtx = Env.WriteTransaction();
        foreach (long id in ids)
            Container.DeleteOrDefer(wtx.LowLevelTransaction, containerId, new ContainerEntryId(id));
        wtx.Commit();
    }

    private long AllocateUntilNextPage(ContainerId containerId, long currentPage)
    {
        using var wtx = Env.WriteTransaction();
        long page;
        do
        {
            page = (long)Container.Allocate(wtx.LowLevelTransaction, containerId, ItemSize, out _) / Constants.Storage.PageSize;
        } while (page == currentPage);

        wtx.Commit();
        return page;
    }

    private static List<long> TakePerPage(List<long> ids, int perPage) =>
        ids.GroupBy(id => id / Constants.Storage.PageSize).SelectMany(page => page.Take(perPage)).ToList();
}
