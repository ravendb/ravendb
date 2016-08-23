using System;
using System.ComponentModel;
using System.Threading;
using Sparrow;
using Sparrow.Utils;
using Xunit;

namespace FastTests.Sparrow
{
    public class ChangeThreadPriorityTest
    {
        [Fact]
        public void StartChangeThreadPriority()
        {
            Exception e = null;
            ThreadPriority threadPriority = ThreadPriority.Normal;
            Thread thread = new Thread(() =>
            {
                try
                {
                    Assert.True(Threading.GetCurrentThreadPriority() == ThreadPriority.Normal);
                    Threading.SetCurrentThreadPriority(ThreadPriority.Highest);
                    lock (this)
                    {
                        threadPriority = Threading.GetCurrentThreadPriority();
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
                Assert.True(threadPriority == ThreadPriority.Highest);
                if (e != null)
                    Assert.False(true, e.ToString());
            }
        }
    }
}