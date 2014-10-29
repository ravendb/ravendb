using System.Collections.Generic;
using System.Linq;
using Voron.Util;
using Xunit;

namespace Voron.Tests.Util
{
	public class ImmutableAppendOnlyTests
	{
		[Fact]
		public void CanAddAndRemove()
		{
			var list = ImmutableAppendOnlyList<long>.Empty
				.Append(1)
				.Append(2);

			var removed = new List<long>();
			list = list.RemoveWhile(x => x <= 1, removed);
			Assert.Equal(1, list.Count);
			Assert.Equal(1, list.Count());
		}
	}
}