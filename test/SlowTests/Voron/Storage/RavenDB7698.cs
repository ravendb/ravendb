// -----------------------------------------------------------------------
//  <copyright file="Quotas.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Storage
{
    public class RavenDB_7698 : FastTests.Voron.StorageTest
    {
        public RavenDB_7698(ITestOutputHelper output) : base(output)
        {
        }

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
                using (var tx2 = tx1.BeginAsyncCommitAndStartNewTransaction(tx1.LowLevelTransaction.PersistentContext,
                    action => Task.Run(() =>
                    {
                        action();
                        return true;
                    })))
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
