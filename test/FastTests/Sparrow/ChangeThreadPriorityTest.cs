using System;
using System.Threading;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public class ChangeThreadPriorityTest : NoDisposalNeeded
    {
        public ChangeThreadPriorityTest(ITestOutputHelper output) : base(output)
        {
        }

        [NonLinuxFact]
        public void StartChangeThreadPriority()
        {
            Exception e = null;
            ThreadPriority threadPriority = ThreadPriority.Normal;
            Thread thread = new Thread(() =>
            {
                try
                {
                    Assert.True(Thread.CurrentThread.Priority == ThreadPriority.Normal);
                    Thread.CurrentThread.Priority = ThreadPriority.Lowest;
                    lock (this)
                    {
                        threadPriority = Thread.CurrentThread.Priority;
                    }
                }
                catch (Exception ex)
                {
                    lock (this)
                    {
                        e = ex;
                    }
                }
            });
            thread.Start();
            thread.Join();
            lock (this)
            {
                Assert.True(threadPriority == ThreadPriority.Lowest);
                if (e != null)
                    Assert.False(true, e.ToString());
            }
        }
    }
}
