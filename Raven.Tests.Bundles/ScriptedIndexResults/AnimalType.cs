using System.Collections.Generic;

namespace Raven.Tests.Bundles.ScriptedIndexResults
{
	public class AnimalType
	{
		public string Id { get; set; }
		public int Count { get; set; }
		public string Description { get; set; }
	}

	public class AnimalTypes
	{
		public AnimalTypes()
		{
			TypeCounts = new Dictionary<string, int>();
		}
		public Dictionary<string, int> TypeCounts { get; set; }
		public string Id { get; set; }
	}
}