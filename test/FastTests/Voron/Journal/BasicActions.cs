// -----------------------------------------------------------------------
//  <copyright file="ForceLogFlushes.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using Xunit;
using Voron;
using Voron.Global;
using Xunit.Abstractions;
using System.Runtime.InteropServices;

namespace FastTests.Voron.Journal
{
    public class BasicActions : StorageTest
    {
        public BasicActions(ITestOutputHelper output) : base(output)
        {
        }

        // all tests here relay on the fact than one log file can contains max 10 pages
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxLogFileSize = 10 * Constants.Storage.PageSize;

            options.ManualFlushing = true;
        }

        [Fact]
        public void CanUseMultipleLogFiles()
        {
            var bytes = new byte[Constants.Storage.PageSize / 4];
            new Random().NextBytes(bytes);

            for (var i = 0; i < 30; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    tree.Add("item/" + i, new MemoryStream(bytes));
                    tx.Commit();
                }
            }

            Assert.True(Env.Journal.Files.Count > 1);

            for (var i = 0; i < 15; i++)
            {
                using (var tx = Env.ReadTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    Assert.NotNull(tree.Read("item/" + i));
                }
            }
        }

        [Fact]
        public void ShouldNotReadUncommittedTransaction()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tx.Commit();
            }
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Add("items/1", StreamFor("values/1"));
                // tx.Commit(); uncommitted transaction
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("foo");
                Assert.Null(tree.Read("items/1"));
            }
        }

        [Fact]
        public void CanFlushDataFromLogToDataFile()
        {
            for (var i = 0; i < 100; i++)
            {

                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    tree.Add("items/" + i, StreamFor("values/" + i));
                    tx.Commit();
                }
            }

            Assert.True(Env.Journal.Files.Count > 1);
            var usedLogFiles = Env.Journal.Files.Count;

            Env.FlushLogToDataFile();

            Assert.True(Env.Journal.Files.Count <= 1 && Env.Journal.Files.Count < usedLogFiles);

            for (var i = 0; i < 100; i++)
            {
                using (var tx = Env.ReadTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    var readKey = ReadKey(tx, tree, "items/" + i);
                    Assert.Equal("values/" + i, readKey.Item2.ToString());
                }
            }
        }
    }
}
