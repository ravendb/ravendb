using System.ComponentModel;
using System.Threading;
using Sparrow;
using Xunit;

namespace FastTests.Sparrow
{
    public class ChangeThreadPriorityTest
    {
        [Fact]
        public void StartChangeThreadPriority()
        {
            ThreadPriority threadPriority = ThreadPriority.Normal;
            Thread thread = new Thread(() =>
            {
                try
                {
                    Assert.True(ThreadMethods.GetThreadPriority() == ThreadPriority.Normal);
                    ThreadMethods.SetThreadPriority(ThreadPriority.Highest);
                    threadPriority = ThreadMethods.GetThreadPriority();

                }
                catch (Win32Exception ex)
                {
                    throw new Win32Exception(ex.Message, ex);
                }
            });
            thread.Start();
            thread.Join();
            Assert.True(threadPriority == ThreadPriority.Highest);
        }
    }
}