using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Raven.Abstractions.Util;
using Raven.Client.Embedded;
using Raven.Database.Extensions;

namespace Raven.Tryouts
{
	static class Program
	{
		static void Main(string[] args)
		{
			IOExtensions.DeleteDirectory("\\FooBar");
			using (var store = new EmbeddableDocumentStore
			{
				DataDirectory = "\\FooBar"
			})
			{
				store.Initialize();

				ThreadPool.SetMinThreads(25, 25);

				using (var bulk = store.BulkInsert())
				{
					for (int i = 1; i < 10001; i++)
					{
						Console.WriteLine(i);
						bulk.Store(new { Foo = i }, "foobar/" + i);
					}
				}

				Console.WriteLine("Warming up, jit, etc.");
				for (int i = 0; i < 5; i++)
				{
					MeasureRunSync(store);
				}

				Console.WriteLine("Start loading test");
				long total = 0;
				int counter = 0;
				Parallel.For(0, 1, _ =>
				{
					for (int i = 0; i < 15; i++)
					{
						Interlocked.Increment(ref counter);
						Interlocked.Add(ref total, MeasureRunSync(store));
					}

				});

				Console.WriteLine("average : " + (total / (decimal)counter));
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
						var index = rand.Next(1, 10000);
						var doc = session.Load<dynamic>("foobar/" + index);
						if (doc.Foo != index)
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