// -----------------------------------------------------------------------
//  <copyright file="ForceLogFlushes.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using Voron.Impl;
using Voron.Impl.Paging;
using Xunit;

namespace Voron.Tests.Journal
{
    public class BasicActions : StorageTest
    {
        // all tests here relay on the fact than one log file can contains max 10 pages
        protected override void Configure(StorageEnvironmentOptions options)
        {
			options.MaxLogFileSize = 10 * AbstractPager.PageSize;
	        options.ManualFlushing = true;
        }

        [Fact]
        public void CanUseMultipleLogFiles()
        {
            var bytes = new byte[1024];
            new Random().NextBytes(bytes);

            for (var i = 0; i < 15; i++)
            {
                using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    tx.State.Root.Add(tx, "item/" + i, new MemoryStream(bytes));
                    tx.Commit();
                }
            }

            Assert.True(Env.Journal.Files.Count > 1);

            for (var i = 0; i < 15; i++)
            {
                using (var tx = Env.NewTransaction(TransactionFlags.Read))
                {
                    Assert.NotNull(tx.State.Root.Read(tx, "item/" + i));
                }
            }
        }

        [Fact]
        public void ShouldNotReadUncommittedTransaction()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.State.Root.Add(tx, "items/1", StreamFor("values/1"));
                // tx.Commit(); uncommitted transaction
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                Assert.Null(tx.State.Root.Read(tx, "items/1"));
            }
        }

        [Fact]
        public void CanFlushDataFromLogToDataFile()
        {
            for (var i = 0; i < 100; i++)
            {
               
                using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    tx.State.Root.Add(tx, "items/" + i, StreamFor("values/" + i));
                    tx.Commit();
                }
            }

            Assert.True(Env.Journal.Files.Count > 1);
            var usedLogFiles = Env.Journal.Files.Count;

            Env.FlushLogToDataFile();

            Assert.True(Env.Journal.Files.Count <= 1 && Env.Journal.Files.Count < usedLogFiles);

            for (var i = 0; i < 100; i++)
            {
                using (var tx = Env.NewTransaction(TransactionFlags.Read))
                {
                    var readKey = ReadKey(tx, "items/" + i);
                    Assert.Equal("values/" + i, readKey.Item2);
                }
            }
        }
    }
}