// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2850.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Xunit;
using Voron;
using Voron.Global;

namespace FastTests.Voron.Bugs
{
    public class RavenDB_2850 : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.PageSize = 4 * Constants.Size.Kilobyte;
            options.MaxScratchBufferSize = Constants.Size.Megabyte * 1 + options.PageSize;
            options.ManualFlushing = true;
        }

        [Fact]
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
                using (var txw = Env.WriteTransaction())
                {
                    var value = new byte[size];

                    random.NextBytes(value);

                    var tree = txw.CreateTree("foo/0");

                    tree.Add("items/", value);

                    txw.Commit();
                }

                size += Env.Options.PageSize;
            }

            for (int i = 0; i < 48; i++)
            {
                using (var txw = Env.WriteTransaction())
                {
                    var tree = txw.CreateTree("foo");


                    var value = new byte[Env.Options.PageSize];

                    random.NextBytes(value);

                    tree.Add("key", value);

                    txw.Commit();
                }
            }

            using (var txw = Env.WriteTransaction())
            {
                var tree = txw.CreateTree("foo/1");

                var value = new byte[40 * Env.Options.PageSize];

                random.NextBytes(value);

                tree.Add("items/", value);

                txw.Commit();
            }

             Env.FlushLogToDataFile();
        }
    }
}