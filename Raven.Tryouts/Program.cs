using System;
using System.Diagnostics;
using Raven.Client.Embedded;
using Raven.SlowTests.Issues;
using Raven.Tests.Core;
using Raven.Tests.Core.Querying;

namespace Raven.Tryouts
{
    public class Program
	{
		private static void Main()
		{
			Console.WriteLine("3.0");
			for (int i = 0; i < 25; i++)
			{
				var sp = Stopwatch.StartNew();
				using (var store = new EmbeddableDocumentStore()
				{
					RunInMemory = true,
					
				}.Initialize())
				{
					store.DatabaseCommands.Get("hello");
				}
				Console.WriteLine(sp.ElapsedMilliseconds);
			}

		}
	}


	
}