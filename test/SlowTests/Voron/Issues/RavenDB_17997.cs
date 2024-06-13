using System;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Impl;
using Voron.Impl.Paging;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues;

public unsafe class RavenDB_17997 : StorageTest
{
    public RavenDB_17997(ITestOutputHelper output) : base(output)
    {
    }

    protected override void Configure(StorageEnvironmentOptions options)
    {
        options.ManualFlushing = true;
        options.ManualSyncing = true;
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void MustNotReadFromUnmappedAllocation()
    {
//         var dataPager = Env.Options.DataPager;
//
//         using (var tempTx = new TempPagerTransaction())
//         {
//             PagerState stateUsedDuringAcquirePagePointer;
//             byte* ptr;
//
//             {
//                 // the below code show the wrong usage that we had in ApplyPagesToDataFileFromScratch() where we're not under write tx
//                 // if we don't pass pager state to AcquirePagePointer() then it will use the one from the pager instance which might get released because of AllocateMorePages() calls
//
//                 /*
//                 var state = dataPager.PagerState;
//                 state.AddRef();
//
//                 dataPager.AllocateMorePages(4 * 1024 * 1024);
//                 dataPager.AllocateMorePages(8 * 1024 * 1024);
//
//                 stateUsedDuringAcquirePagePointer = dataPager.PagerState; 
//
//                 ptr = dataPager.AcquirePagePointer(tempTx, 0);
//                 */
//             }
//
//             {
//                 // correct usage should be to increment reference count of pager state and use that pager in AcquirePagePointer()
//                 stateUsedDuringAcquirePagePointer = dataPager.GetPagerStateAndAddRefAtomically();
//
//                 dataPager.AllocateMorePages(4 * 1024 * 1024);
//                 dataPager.AllocateMorePages(8 * 1024 * 1024);
//
//                 ptr = dataPager.AcquirePagePointer(tempTx, 0, stateUsedDuringAcquirePagePointer);
//             }
//
//             dataPager.AllocateMorePages(16 * 1024 * 1024);
//
//             if (ptr == stateUsedDuringAcquirePagePointer.MapBase && stateUsedDuringAcquirePagePointer._released)
//             {
//                 throw new InvalidOperationException("Cannot read from already unmapped allocation of memory mapped file");
//             }
//         }
        throw new NotImplementedException();
    }
}
