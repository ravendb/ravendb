// -----------------------------------------------------------------------
//  <copyright file="ForScratchBuffer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Xunit;
using Voron;
using Voron.Impl;

namespace FastTests.Voron.ScratchBuffer
{
    public class ScratchCanForceToFlushOldPages: StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.PageSize = 4 * Constants.Size.Kilobyte;
            options.ManualFlushing = true;
            base.Configure(options);
        }

        [Fact]
        public void CanForceToFlushPagesOlderThanOldestActiveTransactionToFreePagesFromScratch()
        {
            using (var txw = Env.WriteTransaction())
            {
                var tree = txw.CreateTree( "foo");

                tree.Add("bars/1", new string('a', 1000));

                txw.Commit();
            }

            using (var txw = Env.WriteTransaction())
            {
                txw.CreateTree("bar");

                txw.Commit();
            }

            using (var txw = Env.WriteTransaction())
            {
                var tree = txw.CreateTree( "foo");

                tree.Add("bars/1", new string('b', 1000));

                txw.Commit();

            }

            var txr = Env.ReadTransaction();
            {
                using (var txw = Env.WriteTransaction())
                {
                    var tree = txw.CreateTree( "foo");

                    tree.Add("bars/1", new string('c', 1000));

                    txw.Commit();

                }

                Env.FlushLogToDataFile();

                txr.Dispose();

                using (var txr2 = Env.ReadTransaction())
                {
                    var allocated1 = Env.ScratchBufferPool.GetNumberOfAllocations(0);

                    Env.FlushLogToDataFile();

                    var allocated2 = Env.ScratchBufferPool.GetNumberOfAllocations(0);

                    Assert.True(allocated2 < allocated1);

                    var read = txr2.CreateTree("foo").Read("bars/1");

                    Assert.NotNull(read);
                    Assert.Equal(new string('c', 1000), read.Reader.AsSlice(txr2.Allocator).ToString());
                }
            }
        } 
    }
}