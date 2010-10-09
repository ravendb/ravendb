using System.IO;
using Raven.Storage.Managed.Data;
using Xunit;
using System.Linq;

namespace Raven.Storage.Tests
{
	public class BagTests
	{
		[Fact]
		public void CanAddToStack()
		{
			var buffer = new byte[1024];
			var stack = new Bag(new MemoryStream(buffer), new MemoryStream(buffer), StartMode.Create);
			stack.Add(1);

			Assert.Equal(1, stack.First());
		}

		[Fact]
		public void CanAddToStackAndReadFromAnother()
		{
			var buffer = new byte[1024];
			var stack = new Bag(new MemoryStream(buffer), new MemoryStream(buffer), StartMode.Create);
			stack.Add(2);
			stack.Flush();

			stack = new Bag(new MemoryStream(buffer)
			{
				Position = stack.CurrentPosition.Value
			}, new MemoryStream(buffer), StartMode.Open);
			Assert.Equal(2, stack.First());
		}

		[Fact]
		public void CanPushSeveralItems()
		{
			var buffer = new byte[1024];
			var stack = new Bag(new MemoryStream(buffer), new MemoryStream(buffer), StartMode.Create);
			stack.Add(1);
			stack.Add(2);
			stack.Add(3);

			Assert.Contains(3, stack);
			Assert.Contains(2, stack);
			Assert.Contains(1, stack);
		}


		[Fact]
		public void CanRemove()
		{
			var buffer = new byte[1024];
			var stack = new Bag(new MemoryStream(buffer), new MemoryStream(buffer), StartMode.Create);
			stack.Add(1);
			stack.Add(2);
			stack.Add(3);
			stack.Remove(2);

			Assert.Contains(3, stack);
			Assert.DoesNotContain(2, stack);
			Assert.Contains(1, stack);
		}

		[Fact]
		public void CanRemoveAfterReopen()
		{
			var buffer = new byte[1024];
			var writer = new MemoryStream(buffer);
			var stack = new Bag(new MemoryStream(buffer), writer, StartMode.Create);
			stack.Add(1);
			stack.Add(2);
			stack.Add(3);
			stack.Flush();

			stack = new Bag(new MemoryStream(buffer)
			{
				Position = stack.CurrentPosition.Value
			}, new MemoryStream(buffer)
			{
				Position = writer.Position
			}, StartMode.Open);

			stack.Remove(2);

			Assert.Contains(3, stack);
			Assert.DoesNotContain(2, stack);
			Assert.Contains(1, stack);
		}

		[Fact]
		public void CanScanSeveralItems()
		{
			var buffer = new byte[1024];
			var stack = new Bag(new MemoryStream(buffer), new MemoryStream(buffer), StartMode.Create);
			stack.Add(1);
			stack.Add(2);
			stack.Add(3);

			Assert.Equal(new long[]{1,2,3}, stack.OrderBy(x=>x).ToArray());
		}


		[Fact]
		public void CanPushSeveralItemsAndScanFromAnother()
		{
			var buffer = new byte[1024];
			var stack = new Bag(new MemoryStream(buffer), new MemoryStream(buffer), StartMode.Create);
			stack.Add(1);
			stack.Add(2);
			stack.Add(3);
			stack.Flush();

			stack = new Bag(new MemoryStream(buffer)
			{
				Position = stack.CurrentPosition.Value
			}, new MemoryStream(buffer), StartMode.Open);

			Assert.Equal(new long[] { 1,2,3 }, stack.OrderBy(x=>x).ToArray());
		}

		[Fact]
		public void CanPushSeveralItemsAndReadFromAnother()
		{
			var buffer = new byte[1024];
			var stack = new Bag(new MemoryStream(buffer), new MemoryStream(buffer), StartMode.Create);
			stack.Add(1);
			stack.Add(2);
			stack.Add(3);
			stack.Flush();

			stack = new Bag(new MemoryStream(buffer)
			{
				Position = stack.CurrentPosition.Value
			}, new MemoryStream(buffer), StartMode.Open);

			Assert.Contains(3, stack);
			Assert.Contains(2, stack);
			Assert.Contains(1, stack);
		}

		[Fact]
		public void CanPushSeveralItemsAndReadFromAnotherThenReadFromAnotherAgain()
		{
			var buffer = new byte[1024];
			var writer = new MemoryStream(buffer);
			var stack = new Bag(new MemoryStream(buffer), writer, StartMode.Create);
			stack.Add( 1 );
			stack.Add( 2 );
			stack.Add( 3 );
			stack.Flush();

			stack = new Bag(new MemoryStream(buffer)
			{
				Position = stack.CurrentPosition.Value
			}, new MemoryStream(buffer)
			{
				Position = writer.Position
			}, StartMode.Open);

			Assert.Contains(3, stack);
			Assert.Contains(2, stack);

			stack.Flush();

			stack = new Bag(new MemoryStream(buffer)
			{
				Position = stack.CurrentPosition.Value
			}, new MemoryStream(buffer)
			{
				Position = writer.Position
			}, StartMode.Open);

			Assert.Contains(1, stack);
		}

	}
}