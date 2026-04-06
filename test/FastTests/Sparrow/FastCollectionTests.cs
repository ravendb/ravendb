using System.Reflection;
using Sparrow.Collections;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public class FastCollectionTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.Core)]
        public void FastStack_CopyTo_UpdatesDestinationCount()
        {
            var src = new FastStack<int>();
            src.Push(10);
            src.Push(20);
            src.Push(30);

            var dst = new FastStack<int>();
            dst.Push(1);
            dst.Push(2);

            dst.CopyTo(src);

            Assert.Equal(5, dst.Count);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void FastStack_CopyTo_ElementsAccessibleAfterCopy()
        {
            var src = new FastStack<int>();
            src.Push(10);
            src.Push(20);

            var dst = new FastStack<int>();
            dst.Push(1);

            dst.CopyTo(src);

            Assert.Equal(3, dst.Count);
            // Pop should return the last-pushed (copied) element
            Assert.Equal(20, dst.Pop());
            Assert.Equal(10, dst.Pop());
            Assert.Equal(1, dst.Pop());
            Assert.Equal(0, dst.Count);
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData(0, false, 0)]   // depth=0 is invalid, must return false
        [InlineData(1, true, 99)]   // depth=1 = top of stack
        [InlineData(2, true, 42)]   // depth=2 = second from top
        [InlineData(3, false, 0)]   // depth exceeds size, must return false
        public void FastStack_TryPeek_Depth(int depth, bool expectedSuccess, int expectedValue)
        {
            var stack = new FastStack<int>();
            stack.Push(42);
            stack.Push(99);

            bool success = stack.TryPeek(depth, out int result);
            Assert.Equal(expectedSuccess, success);
            if (expectedSuccess)
                Assert.Equal(expectedValue, result);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void FastList_RemoveAt_ClearsVacatedSlot()
        {
            var list = new FastList<string>();
            list.Add("a");
            list.Add("b");
            list.Add("c");

            list.RemoveAt(0); // removes "a", shifts "b","c" left → Count=2

            Assert.Equal(2, list.Count);
            Assert.Equal("b", list[0]);
            Assert.Equal("c", list[1]);

            // Use reflection to verify _items[Count] (the vacated slot) is null
            var itemsField = typeof(FastList<string>).GetField("_items", BindingFlags.NonPublic | BindingFlags.Instance);
            var items = (string[])itemsField.GetValue(list);
            Assert.Null(items[list.Count]); // slot at index 2 should be cleared
        }

        [RavenFact(RavenTestCategory.Core)]
        public void FastList_Remove_ClearsVacatedSlot()
        {
            var list = new FastList<string>();
            list.Add("x");
            list.Add("y");
            list.Add("z");

            bool removed = list.Remove("x"); // calls RemoveAt(0) internally
            Assert.True(removed);
            Assert.Equal(2, list.Count);

            var itemsField = typeof(FastList<string>).GetField("_items", BindingFlags.NonPublic | BindingFlags.Instance);
            var items = (string[])itemsField.GetValue(list);
            Assert.Null(items[list.Count]); // slot at index 2 should be cleared
        }
    }
}
