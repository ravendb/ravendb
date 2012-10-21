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
				Map = "from doc in docs select  new { Age = Enumerable.Sum(doc.Children, x=> Enumerable.Sum(x, y=>y.Age))} ",
			}, ".");
			var abstractViewGenerator = x.GenerateInstance();
			var viewText = abstractViewGenerator.SourceCode;
			Console.WriteLine(viewText);
		}
	}
}