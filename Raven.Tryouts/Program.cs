using System;
using Raven.Abstractions.Indexing;
using Raven.Database.Linq;

namespace Raven.Tryouts
{
	internal class Program
	{
		private static void Main()
		{
			var x = new DynamicViewCompiler("test", new IndexDefinition
			{
				Map = "from doc in docs select new { doc.Name, Count = 1 }",
				Reduce = "from result in results group result by result.Name into g select new { Name = g.Key, Count = g.Sum(x=>x.Count) }"
			}, ".");
			var abstractViewGenerator = x.GenerateInstance();
			var viewText = abstractViewGenerator.ViewText;
			Console.WriteLine(viewText);
		}
	}
}