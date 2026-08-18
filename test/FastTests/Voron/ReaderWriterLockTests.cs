using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Voron.Util;
using Xunit;
using Tests.Infrastructure;

namespace FastTests.Voron
{
#pragma warning disable CS0618 // this test covers the obsolete lock while the journal flush still uses it
    public class ReaderWriterLockTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.Core)]
        public async Task WriterWhileHavingMultipleReaders()
        {
            var readWriteLock = new ThreadHoppingReaderWriterLock();
            const int timeoutInMs = 30_000;

            var reader1Wait = new SemaphoreSlim(initialCount: 0);
            var reader1Signal = new SemaphoreSlim(initialCount: 0);
            readWriteLock.ForTestingPurposesOnly().BeforeReaderWriterWait = () =>
            {
                reader1Signal.Release();
                reader1Wait.Wait();
            };

            readWriteLock.EnterWriteLock();

            var reader1Task = Task.Run(() =>
            {
                var sp = Stopwatch.StartNew();
                var lockTaken = readWriteLock.TryEnterReadLock(timeout: TimeSpan.FromMilliseconds(timeoutInMs));
                return (LockTaken: lockTaken, ElapsedInMs: sp.ElapsedMilliseconds);
            });

            reader1Signal.Wait();

            var reader2Signal = new SemaphoreSlim(initialCount: 0);
            var reader2Wait = new SemaphoreSlim(initialCount: 0);
            readWriteLock.ForTestingPurposesOnly().BeforeResetOfReaderWait = () =>
            {
                reader2Signal.Release();
                reader2Wait.Wait();
            };

            var reader2 = Task.Run(() =>
            {
                var sp = Stopwatch.StartNew();
                var lockTaken = readWriteLock.TryEnterReadLock(timeout: TimeSpan.FromMilliseconds(timeoutInMs));
                return (LockTaken: lockTaken, ElapsedInMs: sp.ElapsedMilliseconds);
            });


            reader2Signal.Wait();

            readWriteLock.ExitWriteLock();

            reader2Wait.Release();

            var resultReader2 = await reader2;
            Assert.True(resultReader2.LockTaken);
            Assert.True(resultReader2.ElapsedInMs < 1000, $"elapsed: {resultReader2.ElapsedInMs}");

            reader1Wait.Release();

            var resultReader1 = await reader1Task;
            Assert.True(resultReader1.LockTaken);
            Assert.True(resultReader1.ElapsedInMs < 1000, $"elapsed: {resultReader1.ElapsedInMs}");
        }
    }
#pragma warning restore CS0618
}
