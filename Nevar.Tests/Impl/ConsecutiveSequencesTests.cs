using Nevar.Impl;
using Xunit;

namespace Nevar.Tests.Impl
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
			var cs = new ConsecutiveSequences();
			cs.Add(5);
			long l;
			Assert.True(cs.TryAllocate(1, out l));
			Assert.Equal(5, l);
		}

		[Fact]
		public void WhenHasSeqWillReturnIt()
		{
			var cs = new ConsecutiveSequences();
			cs.Add(5);
			cs.Add(6);
			long l;
			Assert.True(cs.TryAllocate(1, out l));
			Assert.Equal(5, l);
			Assert.True(cs.TryAllocate(1, out l));
			Assert.Equal(6, l);
		} 

		[Fact]
		public void WillDetectSequenceOutOfOrder1()
		{
			var cs = new ConsecutiveSequences();
			cs.Add(5);
			cs.Add(18);
			cs.Add(6);

			long l;
			Assert.True(cs.TryAllocate(1, out l));
			Assert.Equal(5, l);
			Assert.True(cs.TryAllocate(1, out l));
			Assert.Equal(6, l);
			Assert.True(cs.TryAllocate(1, out l));
			Assert.Equal(18, l);
		}

		[Fact]
		public void WillDetectSequenceOutOfOrder2()
		{
			var cs = new ConsecutiveSequences();
			cs.Add(6);
			cs.Add(18);
			cs.Add(5);

			long l;
			Assert.True(cs.TryAllocate(1, out l));
			Assert.Equal(5, l);
			Assert.True(cs.TryAllocate(1, out l));
			Assert.Equal(6, l);
			Assert.True(cs.TryAllocate(1, out l));
			Assert.Equal(18, l);
		}
	}
}