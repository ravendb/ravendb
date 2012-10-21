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
				Map = "docs.Items.Select(doc => new { doc.Name, Count = 1 });",
				Reduce = "results.GroupBy(result=>result.Name).Select(g=> new { Name = g.Key, Count = g.Sum(x=>x.Count) }.Boost(5));"
			}, ".");
			var abstractViewGenerator = x.GenerateInstance();
			var viewText = abstractViewGenerator.ViewText;
			Console.WriteLine(viewText);
		}
	}
}