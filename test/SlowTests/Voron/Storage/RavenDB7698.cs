// -----------------------------------------------------------------------
//  <copyright file="Quotas.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Voron;
using Xunit;

namespace SlowTests.Voron.Storage
{
    public class RavenDB_7698 : FastTests.Voron.StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [Fact]
        public void CanRestartEmptyAsyncTransaction()
        {
            RequireFileBasedPager();

            var tx1 = Env.WriteTransaction();

            try
            {
                using (var tx2 = tx1.BeginAsyncCommitAndStartNewTransaction())
                {
                    using (tx1)
                    {
                        tx2.CreateTree("test");

                        tx1.EndAsyncCommit();
                    }

                    tx1 = null;

                    tx2.Commit();
                }
            }
            finally
            {
                tx1?.Dispose();
            }
            
            RestartDatabase();
        }
    }
}
