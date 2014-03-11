using Raven.Tryouts;
using Xunit;

namespace Raven.StressTests.Races
{
	public class Georgios : StressTest
	{
		[Fact]
		public void FactMethodName()
		{
			Run<AvoidRaceConditionWhenWeLoadTheDataNotPatched>(patched => patched.GetReturnsFilteredResults(), 200);
		}
	}
}