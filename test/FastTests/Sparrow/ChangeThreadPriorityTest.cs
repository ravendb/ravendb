using System.Threading;
using Sparrow;
using Xunit;

namespace FastTests.Sparrow
{
    public class ChangeThreadPriorityTest
    {
        [Fact]
        public static void StartChangeThreadPriority()
        {
            Thread first = new Thread(() => ChangeThreadPriorityCheck("First"));
            Thread second = new Thread(() => ChangeThreadPriorityCheck("Second"));
            Thread third = new Thread(() => ChangeThreadPriorityCheck("Third"));

            first.Start();
            second.Start();
            third.Start();
        }

        private static void ChangeThreadPriorityCheck(string threadName)
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
    }
}
