using System;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using Sparrow;
using Xunit;

namespace FastTests.Sparrow
{
    public class ChangeThreadPriorityTest
    {
        [Fact]
        public void StartChangeThreadPriority()
        {
            Task first = Task.Run(() => ChangeThreadPriorityCheck("First"));
            Task second = Task.Run(() => ChangeThreadPriorityCheck("Second"));
            Task third = Task.Run(() => ChangeThreadPriorityCheck("Third"));

            Task.WaitAll(first, second, third);

        }

        private void ChangeThreadPriorityCheck(string threadName)
        {
            try
            {
                Assert.True(ThreadMethods.GetThreadPriority() == ThreadPriority.Normal);

                switch (threadName)
                {
                    case "First":
                        ThreadMethods.SetThreadPriority(ThreadPriority.Highest);
                        Assert.True(ThreadMethods.GetThreadPriority() == ThreadPriority.Highest);
                        break;
                    case "Second":
                        ThreadMethods.SetThreadPriority(ThreadPriority.BelowNormal);
                        Assert.True(ThreadMethods.GetThreadPriority() == ThreadPriority.BelowNormal);
                        break;
                    case "Third":
                        ThreadMethods.SetThreadPriority(ThreadPriority.Lowest);
                        Assert.True(ThreadMethods.GetThreadPriority() == ThreadPriority.Lowest);
                        break;
                }
            }
            catch (Win32Exception ex)
            {
                Console.WriteLine(ex);
            }
            
        }
    }
}
