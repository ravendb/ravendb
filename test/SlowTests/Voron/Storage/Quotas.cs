// -----------------------------------------------------------------------
//  <copyright file="Quotas.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using Voron;
using Voron.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Storage
{
    public class Quotas : FastTests.Voron.StorageTest
    {
        public Quotas(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxStorageSize = 1024 * 1024 * 1; // 1MB
        }

        [Fact]
        public void ShouldThrowQuotaException()
        {
            var quotaEx = Assert.Throws<QuotaException>(() =>
            {
                // everything in one transaction
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    for (int i = 0; i < 1024; i++)
                    {
                        tree.Add("items/" + i, new MemoryStream(new byte[1024]));
                    }

                    tx.Commit();
                }
            });
        }
    }
}
