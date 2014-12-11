using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Database.Config;

namespace Raven.Tryouts
{
	public class Program
	{
		//average - voron 271ms
		static void Main(string[] args)
		{
			var creationTimings = new List<long>();
			for (var i = 0; i < 100; i++)
			{
				var sw = Stopwatch.StartNew();
				using (CreateAndInitStore())
				{
					long elapsedMilliseconds = sw.ElapsedMilliseconds;
					Console.WriteLine("{0}ms", elapsedMilliseconds);
					creationTimings.Add(elapsedMilliseconds);
				}
			}

			Console.WriteLine("Average timing: {0}ms", creationTimings.Average());
		}

		private static IDocumentStore CreateAndInitStore()
		{
			var store = new EmbeddableDocumentStore
			{
				RunInMemory = true,				
			};
			store.Initialize();

			return store;
		}
	}



}