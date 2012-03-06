using Raven.Tests.Shard.BlogModel;
using Xunit;

namespace Raven.StressTests.Races
{
	public class ShardsRaces : StressTest
	{
		[Fact]
		public void CanQueryOnlyUsers_WhenStoringUser()
		{
			Run<CanQueryOnlyUsers>(x => x.WhenStoringUser());
		}
	
		[Fact]
		public void CanQueryOnlyUsers_WhenQueryingForUserById()
		{
			Run<CanQueryOnlyUsers>(x => x.WhenQueryingForUserById());
		}

		[Fact]
		public void CanQueryOnlyUsers_WhenQueryingForUserByName()
		{
			Run<CanQueryOnlyUsers>(x => x.WhenQueryingForUserByName());
		}
	}
}