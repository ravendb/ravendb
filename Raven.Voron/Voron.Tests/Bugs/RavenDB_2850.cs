// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2850.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Voron.Impl.Paging;
using Xunit;

namespace Voron.Tests.Bugs
{
    public class RavenDB_2850 : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxScratchBufferSize = 1024*1024*1;
            options.ManualFlushing = true;
        }

        [PrefixesFact]
        public void FlushingLogsShouldNotCauseExceptions()
        {
            // this test reproduces the issue RavenDB-2850 - very rare corner case that involves the journal applicator and the scratch buffer
            // Issue description: during ApplyLogsToDataFile (operation #1) we create write transactions to be sure that nobody else does writes at the same time.
            // However the problem is that write transactions allocate a page for transactions header. During this allocation by using the scratch buffer we might notice 
            // that we are close to the scratch buffer size limit, so we are forcing ApplyLogsToDataFile (operation #2). This operation is performed on the same thread so
            // locking mechanism will not prevent it from doing this.
            // Operation #2 applies pages, removes journal files that are not longer in use and successfully finishes. Scratch buffer continues the allocation and returns
            // a page for the transaction header initiated by operation #1. Operation #1 keep working but during an attempt to delete old journals it throws
            // InvalidOperationException("Sequence contains no matching element") because operation #2 already did it.

            var random = new Random(1);

            var size = 0;

            while (Env.ScratchBufferPool.GetPagerStatesOfAllScratches().Count < 2)
            {
                using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var value = new byte[size];

                    random.NextBytes(value);

                    txw.Root.Add("items/", value);

                    txw.Commit();
                }

                size += AbstractPager.PageSize;
            }

            for (int i = 0; i < 48; i++)
            {
                using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var tree = Env.CreateTree(txw, "foo");

                    var value = new byte[AbstractPager.PageSize];

                    random.NextBytes(value);

                    tree.Add("key", value);

                    txw.Commit();
                }
            }

            using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = Env.CreateTree(txw, "foo/1");

                var value = new byte[40 * AbstractPager.PageSize];

                random.NextBytes(value);

                tree.Add("items/", value);

                txw.Commit();
            }

            Assert.DoesNotThrow(() =>  Env.FlushLogToDataFile());
        }
    }
}
