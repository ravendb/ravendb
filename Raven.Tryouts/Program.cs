using Raven.StressTests.Races;

namespace Raven.Tryouts
{
	class Program
	{
		private static void Main()
		{
			new BundelsRaceConditions().FailoverBetweenTwoMultiTenantDatabases_CanReplicateBetweenTwoMultiTenantDatabases();
		}
	}
}