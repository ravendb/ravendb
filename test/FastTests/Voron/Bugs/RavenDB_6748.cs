﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Voron;
using Xunit;

namespace FastTests.Voron.Bugs
{
    public class RavenDB_6748 : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [Fact]
        public unsafe void Must_not_release_scratch_pages_which_are_visible_for_read_transaction()
        {
            using (var txw = Env.WriteTransaction())
            {
                for (int i = 0; i < 20; i++)
                {
                    txw.LowLevelTransaction.AllocatePage(1);
                }
                
                txw.Commit();
            }

            var trackedPageNumber = 13;

            using (var txw = Env.WriteTransaction())
            {
                var allocatePage = txw.LowLevelTransaction.ModifyPage(trackedPageNumber);

                allocatePage.DataPointer[0] = 1;
                allocatePage.DataPointer[1] = 2;
                allocatePage.DataPointer[2] = 3;

                txw.Commit();
            }

            for (int i = 1; i < 5; i++)
            {
                using (var txw = Env.WriteTransaction())
                {
                    txw.LowLevelTransaction.ModifyPage(trackedPageNumber + i);

                    txw.Commit();
                }
            }

            using (var txr = Env.ReadTransaction())
            {
                using (var txw = Env.WriteTransaction())
                {
                    txw.LowLevelTransaction.ModifyPage(trackedPageNumber);

                    txw.Commit();
                }

                Env.FlushLogToDataFile();

                using (var txw = Env.WriteTransaction())
                {
                    for (int i = 0; i < 20; i++)
                    {
                        txw.LowLevelTransaction.AllocatePage(1, zeroPage: true);
                    }

                    txw.Commit();
                }

                var page = txr.LowLevelTransaction.GetPage(13);

                Assert.Equal(trackedPageNumber, page.PageNumber);

                Assert.Equal(1, page.DataPointer[0]);
                Assert.Equal(2, page.DataPointer[1]);
                Assert.Equal(3, page.DataPointer[2]);
            }
        }
    }
}