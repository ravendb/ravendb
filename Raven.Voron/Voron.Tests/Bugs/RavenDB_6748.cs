// -----------------------------------------------------------------------
//  <copyright file="RavenDB_6748.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Voron.Impl;
using Voron.Trees;
using Xunit;

namespace Voron.Tests.Bugs
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
            using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                for (int i = 0; i < 20; i++)
                {
                    txw.AllocatePage(1, PageFlags.None);
                }

                txw.Commit();
            }

            var trackedPageNumber = 13;

            using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var allocatePage = txw.ModifyPage(trackedPageNumber, new Tree(txw, new TreeMutableState()), null);

                var dataPtr = allocatePage.Base + Constants.PageHeaderSize;

                dataPtr[0] = 1;
                dataPtr[1] = 2;
                dataPtr[2] = 3;

                txw.Commit();
            }

            for (int i = 1; i < 5; i++)
            {
                using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    txw.ModifyPage(trackedPageNumber + i, new Tree(txw, new TreeMutableState()), null);

                    txw.Commit();
                }
            }
            Transaction txr2 = null;

            try
            {
                using (var txr = Env.NewTransaction(TransactionFlags.Read))
                {
                    using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        txw.ModifyPage(0, new Tree(txw, new TreeMutableState()), null);

                        txw.Commit();
                    }

                    txr2 = Env.NewTransaction(TransactionFlags.Read);

                    using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        txw.ModifyPage(trackedPageNumber, new Tree(txw, new TreeMutableState()), null);

                        txw.Commit();
                    }

                    Env.FlushLogToDataFile();

                    using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        for (int i = 0; i < 20; i++)
                        {
                            txw.AllocatePage(1, PageFlags.None);
                        }

                        txw.Commit();
                    }

                    var page = txr.GetReadOnlyPage(13);

                    Assert.Equal(trackedPageNumber, page.PageNumber);

                    var dataPtr = page.Base + Constants.PageHeaderSize;

                    Assert.Equal(1, dataPtr[0]);
                    Assert.Equal(2, dataPtr[1]);
                    Assert.Equal(3, dataPtr[2]);
                }

                using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    for (int i = 0; i < 20; i++)
                    {
                        txw.AllocatePage(1, PageFlags.None);
                    }

                    txw.Commit();
                }

                var page2 = txr2.GetReadOnlyPage(13);

                Assert.Equal(trackedPageNumber, page2.PageNumber);

                var dataPtr2 = page2.Base + Constants.PageHeaderSize;

                Assert.Equal(1, dataPtr2[0]);
                Assert.Equal(2, dataPtr2[1]);
                Assert.Equal(3, dataPtr2[2]);
            }
            finally
            {
                if (txr2 != null)
                    txr2.Dispose();
            }
        }
    }
}