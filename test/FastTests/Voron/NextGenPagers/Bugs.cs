using System;
using System.Collections.Generic;
using System.IO;
using FastTests.Voron.FixedSize;
using Newtonsoft.Json;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Voron.Global;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.NextGenPagers;

public class Bugs : StorageTest
{
    public Bugs(ITestOutputHelper output) : base(output)
    {
    }

    
    [RavenFact(RavenTestCategory.Voron)]
    public void FreeSpaceShouldHandleAllocationsOnSectionBoundary()
    {
        using  var txw = Env.WriteTransaction();
        long[] freePages =
        [
            3006, 3007, 3008, 3009, 3104, 3126, 3369, 3370, 3371, 3372, 3375, 3417, 3418, 3419, 3420, 3421, 3586, 3587, 3588, 3589, 3590, 3591, 3592, 3593, 3606, 3677,
            3825, 3826, 3827, 3828, 3829, 3830, 3831, 3832, 3833, 3836, 3837, 3847, 3848, 3858, 3859, 3860, 3861, 3862, 3863, 3864, 3865, 3866, 3899, 3900, 3901, 3931,
            3932, 3933, 4065, 4066, 4076, 4077, 4087, 4088, 4089, 4090, 4091, 4092, 4093, 4094, 4095, 4096, 4097, 4153, 4154, 4155, 4156, 4157, 4303, 4304, 4305, 4306,
            4307, 4308, 4309, 4310, 4311, 4312, 4313, 4314, 4315, 4316, 4317, 4318, 4319, 4320, 4321, 4322, 4323, 4324, 4325, 4326, 4327, 4328, 4329, 4330, 4331, 4332,
            4333, 4334, 4335, 4336, 4337, 4338, 4339, 4340, 4341, 4342, 4343, 4344, 4345, 4346, 4347, 4348, 4349, 4350, 4351, 4352, 4353, 4354, 4355, 4356, 4357, 4358,
            4359, 4360, 4361, 4362, 4363, 4364, 4365, 4366, 4367, 4368, 4369, 4370, 4371, 4372, 4373, 4374, 4375, 4376, 4377, 4378, 4379, 4380, 4381
        ];
        foreach (long fp in freePages)
        {
            Env.FreeSpaceHandling.FreePage(txw.LowLevelTransaction, fp);
        }

        long? value = Env.FreeSpaceHandling.TryAllocateFromFreeSpace(txw.LowLevelTransaction, 16);
        Assert.NotNull(value);
        for (int i = 0; i < 16; i++)
        {
            Assert.Contains(value.Value + i, freePages);
        }
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

    [RavenMultiplatformFact(RavenTestCategory.Voron, RavenArchitecture.All64Bits)]
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
