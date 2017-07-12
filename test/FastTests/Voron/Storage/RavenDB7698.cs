// -----------------------------------------------------------------------
//  <copyright file="Quotas.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using Xunit;
using Voron;
using Voron.Exceptions;

namespace FastTests.Voron.Storage
{
    public class RavenDB_7698 : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [Fact]
        public void CanRestartEmptyAsyncTransaction()
        {
            RequireFileBasedPager();

            using (var tx1 = Env.WriteTransaction())
            using (var tx2 = tx1.BeginAsyncCommitAndStartNewTransaction())
            {
                tx2.CreateTree("test");

                tx1.EndAsyncCommit();

                tx2.Commit();
            }

            RestartDatabase();
        }
    }
}