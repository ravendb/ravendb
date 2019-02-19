// -----------------------------------------------------------------------
//  <copyright file="Mvcc.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using Xunit;
using Voron;
using System.Linq;
using Voron.Global;

namespace FastTests.Voron.Journal
{
    public class Mvcc : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.MaxLogFileSize = 3 * Constants.Storage.PageSize;
        }

        [Fact]
        public void ShouldNotFlushUntilThereAreActiveOlderTransactions()
        {
            var ones = new byte[3000];
            var nines = new byte[3000];

            for (int i = 0; i < 3000; i++)
            {
                ones[i] = 1;
                nines[i] = 9;
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Add("items/1", new MemoryStream(ones));
                tree.Add("items/2", new MemoryStream(ones));
                tx.Commit();
            }

            Env.FlushLogToDataFile(); // make sure that pages where items/1 is contained will be flushed to the data file

            using (var txr = Env.ReadTransaction())
            {
                var treeR = txr.CreateTree("foo");
                using (var txw = Env.WriteTransaction())
                {
                    var treeW = txw.CreateTree("foo");
                    treeW.Add("items/1", new MemoryStream(nines));
                    txw.Commit();
                }

                Env.FlushLogToDataFile(); // should not flush pages of items/1 because there is an active read transaction

                var readResult = treeR.Read("items/1");


                var bytes = readResult.Reader.ReadBytes(readResult.Reader.Length);
                var readData = bytes.Array.Skip(bytes.Offset).Take(bytes.Count).ToArray();

                for (int i = 0; i < 3000; i++)
                {
                    Assert.Equal(1, readData[i]);
                }
            }
        }
    }
}