using Voron.Impl;
using Xunit;

namespace Voron.Tests.Impl
{
	public class ConsecutiveSequencesTests
	{
		[Fact]
		public void WhenThereAreNoItemsCannotAllocate()
		{
			var cs = new ConsecutiveSequences();
			long l;
			Assert.False(cs.TryAllocate(1, out l));
		}

		[Fact]
		public void WhenHasOneItemWillReturnIt()
		{
			var cs = new ConsecutiveSequences {5};
			long l;
			Assert.True(cs.TryAllocate(1, out l));
			Assert.Equal(5, l);
		}

		[Fact]
		public void WhenHasSeqWillReturnIt()
		{
			var cs = new ConsecutiveSequences {5, 6};
			long l;
			Assert.True(cs.TryAllocate(1, out l));
			Assert.Equal(5, l);
			Assert.True(cs.TryAllocate(1, out l));
			Assert.Equal(6, l);
		} 

		[Fact]
		public void WillDetectSequenceOutOfOrder1()
		{
			var cs = new ConsecutiveSequences {5, 18, 6};

			long l;
			Assert.True(cs.TryAllocate(1, out l));
			Assert.Equal(18, l);
			Assert.True(cs.TryAllocate(1, out l));
			Assert.Equal(5, l);
			Assert.True(cs.TryAllocate(1, out l));
			Assert.Equal(6, l);

		}

		[Fact]
		public void WillDetectSequenceOutOfOrder2()
		{
			var cs = new ConsecutiveSequences {6, 18, 5};

			long l;
			Assert.True(cs.TryAllocate(1, out l));
			Assert.Equal(18, l);
			Assert.True(cs.TryAllocate(1, out l));
			Assert.Equal(5, l);
			Assert.True(cs.TryAllocate(1, out l));
			Assert.Equal(6, l);
		}
	}
}