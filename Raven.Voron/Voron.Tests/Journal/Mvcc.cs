// -----------------------------------------------------------------------
//  <copyright file="Mvcc.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using System.Linq;
using Voron.Impl;
using Voron.Impl.Paging;
using Xunit;

namespace Voron.Tests.Journal
{
    public class Mvcc : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.MaxLogFileSize = 3 * AbstractPager.PageSize;
        }

        [PrefixesFact]
        public void ShouldNotFlushUntilThereAreActiveOlderTransactions()
        {
            var ones = new byte[3000];
            var nines = new byte[3000];

            for (int i = 0; i < 3000; i++)
            {
                ones[i] = 1;
                nines[i] = 9;
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.Root.Add			("items/1", new MemoryStream(ones));
                tx.Root.Add			("items/2", new MemoryStream(ones));
                tx.Commit();
            }

            Env.FlushLogToDataFile(); // make sure that pages where items/1 is contained will be flushed to the data file

            using (var txr = Env.NewTransaction(TransactionFlags.Read))
            {
                using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    txw.Root.Add("items/1", new MemoryStream(nines));
                    txw.Commit();
                }

                Env.FlushLogToDataFile(); // should not flush pages of items/1 because there is an active read transaction

                var readResult = txr.Root.Read("items/1");

                int used;
                var readData = readResult.Reader.ReadBytes(readResult.Reader.Length, out used).Take(used).ToArray();

                for (int i = 0; i < 3000; i++)
                {
                    Assert.Equal(1, readData[i]);
                }
            }
        }
    }
}
