using Raven.StressTests.Races;

namespace Raven.Tryouts
{
	class Program
	{
		private static void Main()
		{
			new RaceConditions().SupportLazyOperations_LazyOperationsAreBatched();
		}
	}
}