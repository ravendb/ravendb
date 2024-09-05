using System;
using FastTests.Voron.FixedSize;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.NextGenPagers;

public class Bugs : StorageTest
{
    public Bugs(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void SplitRootPageWhileReading()
    {
        using (var txw = Env.WriteTransaction())
        {
            int i=0;
            Tree rootObjects = txw.LowLevelTransaction.RootObjects;
            while (rootObjects.ReadHeader().RootPageNumber == 0)
            {
                rootObjects.Add(i.ToString(), i.ToString());
                i++;
            }

            using (var txr = Env.ReadTransaction())
            {
                txr.LowLevelTransaction.RootObjects.Read("1");
            }
        }
    }

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineDataWithRandomSeed(100000)]
    [InlineDataWithRandomSeed(200000)]
    public void FuzzySplitRootPageWhileReading(int count = 100000, int seed = 1337)
    {
        var generator = new Random(seed);

        int lastCommitted = -1;
        
        var txw = Env.WriteTransaction();
        Tree rootObjects = txw.LowLevelTransaction.RootObjects;
        for (int i = 0; i < count; i++)
        {
            rootObjects.Add(i.ToString(), i.ToString());

            int action = generator.Next(100);
            if (action == 0)
            {
                txw.Commit();
                txw.Dispose();
                lastCommitted = i;

                txw = Env.WriteTransaction();
                rootObjects = txw.LowLevelTransaction.RootObjects;
            }

            if (action > 90)
            {
                using (var txr = Env.ReadTransaction())
                {
                    for (int j = 0; j < 10; j++)
                    {
                        if (lastCommitted > 0)
                        {
                            int check = generator.Next(lastCommitted);
                            Assert.NotNull(txr.LowLevelTransaction.RootObjects.Read(check.ToString()));

                            check = generator.Next(count - lastCommitted) + lastCommitted + 1;
                            Assert.Null(txr.LowLevelTransaction.RootObjects.Read(check.ToString()));
                        }
                    }
                }
            }
        }

        txw.Commit();
        txw.Dispose();
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CanHandleRollbackOfPageInScratches()
    {
        Options.ManualFlushing = true;

        long pageNum;
        using (var txw = Env.WriteTransaction())
        {
            // here we force the database to grow
            pageNum = txw.LowLevelTransaction.DataPagerState.NumberOfAllocatedPages + 10;
            while (true)
            {
                var p = txw.LowLevelTransaction.AllocatePage(1);
                if (p.PageNumber == pageNum)
                    break;
            }
            txw.Commit();
        }

        using (var txwOne = Env.WriteTransaction())
        {
            txwOne.LowLevelTransaction.ModifyPage(pageNum-1);

            using (var txwTwo = txwOne.BeginAsyncCommitAndStartNewTransaction(new TransactionPersistentContext()))
            {
                txwTwo.LowLevelTransaction.ModifyPage(pageNum);
                
                txwOne.EndAsyncCommit();
                
                // implicit rollback
            }
        }

        using (var txw = Env.WriteTransaction())
        {
            txw.LowLevelTransaction.GetPage(pageNum-1);
            txw.LowLevelTransaction.GetPage(pageNum);
        }
    }
}
