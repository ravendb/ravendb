using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Raven.Abstractions.Util;
using Raven.Client.Embedded;
using Raven.Database.Extensions;
using Raven.Tests.Issues.Prefetcher;

namespace Raven.Tryouts
{
	static class Program
	{
		static void Main(string[] args)
		{
		    for (int i = 0; i < 10; i++)
		    {
		        Console.WriteLine(i);

		        using (var x = new RavenDB_3581())
		        {
                    x.DisposeShouldCleanFutureBatches();
		        }
		    }
		}

		private static long MeasureRunSync(EmbeddableDocumentStore store)
		{
			var rand = new Random(123);
			var sw = Stopwatch.StartNew();
			for (int i = 0; i < 100; i++)
			{
				using (var session = store.OpenSession())
				{
					for (int j = 0; j < 15; j++)
					{
						var index = rand.Next(1, 1000);
						var id = index % 2 == 0 ? "foobar/" + index : "foobar/A";
                        var doc = session.Load<dynamic>(id);
						if (doc != null && doc.Foo != index)
							throw new Exception("Got bad data -> got " + doc.Foo + ", but should be " + index);
					}
				}
			}
			sw.Stop();
			Console.WriteLine(sw.ElapsedMilliseconds);
			return sw.ElapsedMilliseconds;
		}
	}
}