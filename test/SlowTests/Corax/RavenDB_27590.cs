using System;
using System.Collections.Generic;
using System.Linq;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron.Data.PostingLists;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class RavenDB_27590(ITestOutputHelper output) : StorageTest(output)
{
    private const string Name = "entries";


    [RavenFact(RavenTestCategory.Voron)]
    public void PagesAddedAfterTheParentSplitMustFollowTheLeafThatOverflowed()
    {
        const long step = 1L << 20;
        var model = new SortedSet<long>();
        long next = step;

        while (true)
        {
            using (var wtx = Env.WriteTransaction())
            {
                var list = wtx.OpenPostingList(Name);
                for (int i = 0; i < 100_000; i++)
                {
                    list.Add(next);
                    model.Add(next);
                    next += step;
                }

                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                if (rtx.OpenPostingList(Name).State.Depth == 3)
                    break;
            }
        }

        var rootChildren = Children(RootPage());
        long leafStart = Children(rootChildren[0].Page)[1].Key;

        using (var wtx = Env.WriteTransaction())
        {
            var list = wtx.OpenPostingList(Name);
            for (long value = leafStart + 2; value < leafStart + step; value += 2)
            {
                list.Add(value);
                model.Add(value);
            }

            wtx.Commit();
        }

        Assert.Equal(rootChildren.Length + 1, Children(RootPage()).Length);

        using (var rtx = Env.ReadTransaction())
        {
            var list = rtx.OpenPostingList(Name);
            Assert.Equal(model.ToList(), AllValues(list));
        }
    }

    private long RootPage()
    {
        using (var rtx = Env.ReadTransaction())
            return rtx.OpenPostingList(Name).State.RootPage;
    }

    private unsafe (long Key, long Page)[] Children(long pageNumber)
    {
        using (var rtx = Env.ReadTransaction())
        {
            var list = rtx.OpenPostingList(Name);
            var branch = new PostingListBranchPage(list.Llt.GetPage(pageNumber));
            var children = new (long, long)[branch.Header->NumberOfEntries];
            for (int i = 0; i < children.Length; i++)
                children[i] = branch.GetByIndex(i);
            return children;
        }
    }

    private static List<long> AllValues(PostingList list)
    {
        var it = list.Iterate();
        var result = new List<long>();
        Span<long> buffer = stackalloc long[1024];
        if (it.Seek(0) == false)
            return result;
        while (it.Fill(buffer, out int read) && read != 0)
        {
            for (int i = 0; i < read; i++)
                result.Add(buffer[i]);
        }

        return result;
    }
}
