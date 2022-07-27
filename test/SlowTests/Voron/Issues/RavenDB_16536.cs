using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using FastTests.Voron;
using Sparrow;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_16536 : StorageTest
    {
        public RavenDB_16536(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.MaxNumberOfPagesInJournalBeforeFlush = 2; // low value to ensure it will flush
        }

        [Fact]
        public void ShouldNotSayThatThereIsNothingToFlush()
        {
            for (int i = 0; i < 100; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("items");

                    tree.Add("items/" + i, new byte[] { 1, 2, 3 });

                    tx.Commit();
                }
            }

            var onLogsAppliedMre = new ManualResetEventSlim(false);

            Env.OnLogsApplied += () => onLogsAppliedMre.Set();

            var ownFlusher = new GlobalFlushingBehavior();

            ownFlusher.ForTestingPurposesOnly().AllowToFlushEvenIfManualFlushingSet.Add(Env);

            using (Env.ReadTransaction()) // we're holding open read transaction so it won't be able to flush everything
            {

                ownFlusher.ForTestingPurposesOnly().AddEnvironmentToFlushQueue(Env);

                ownFlusher.ForTestingPurposesOnly().ForceFlushEnvironment();

                Assert.True(onLogsAppliedMre.Wait(Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(30)));
            }

            Assert.True(Env.Journal.Applicator.ShouldFlush);

            onLogsAppliedMre.Reset();


            ownFlusher.ForTestingPurposesOnly().AddEnvironmentToFlushQueue(Env);

            // to ignore the internal check that verifies that we are not flushing too often
            // setting it explicitly here instead of via options because via options 30 seconds is the minimum value

            Env.TimeToSyncAfterFlushInSec = 0;

            // the internal check has seconds precision so let's wait 1 sec to ensure the env will be really flushed
            Thread.Sleep(1000);


            ownFlusher.ForTestingPurposesOnly().ForceFlushEnvironment();

            Assert.True(onLogsAppliedMre.Wait(Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(30)));

            Assert.False(Env.Journal.Applicator.ShouldFlush);
        }
    }
}
