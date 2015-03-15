using System;
using Raven.Abstractions.Util;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Core
{
	public class RavenGCTests : RavenTestBase
	{
		[Fact]
		public void Before_and_after_memory_allocation_should_be_recorded_correctly()
		{
			WeakReference reference;
			new Action(() =>
			{
				var bytes = new byte[4096 * 1024];
				new Random().NextBytes(bytes);
				reference = new WeakReference(bytes, true);
			})();

			RavenGC.CollectGarbage(true, () => { },true);

			Assert.True(RavenGC.MemoryBeforeLastForcedGC > RavenGC.MemoryAfterLastForcedGC);	
		}

		//short period of time assumed to be passed between releases
		[Fact]
		public void GC_should_not_occur_twice_if_not_enough_memory_released()
		{
// ReSharper disable once RedundantAssignment
			WeakReference reference;
			var allocateStuff = new Action<int>(sizeToAllocate =>
			{
				var bytes = new byte[sizeToAllocate];
				new Random().NextBytes(bytes);
				reference = new WeakReference(bytes, true);
			});			

			allocateStuff(4096 * 1024);
			Assert.True(RavenGC.CollectGarbage(true, () => { }));

			//sanity check
			Assert.True(RavenGC.MemoryBeforeLastForcedGC > RavenGC.MemoryAfterLastForcedGC);	

			allocateStuff(1024);
			//this time the collection _should_ occur, since
			//last time it had freed more than 10% of memory
			Assert.True(RavenGC.CollectGarbage(true, () => { }));

			//now it should _not_ do the GC, since last time no memory was GC'd
			Assert.False(RavenGC.CollectGarbage(true, () => { }));
		}
	}
}
