
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;

namespace Raven.Tryouts
{
	public class Foo
	{
		public string Bar { get; set; }
	}

	public class SimpleReduceResult
	{
		public string Bar { get; set; }
		public long Count { get; set; }
	}

	public class SimpleMRIndex : AbstractIndexCreationTask<Foo,SimpleReduceResult>
	{
		public SimpleMRIndex()
		{
			Map = foos => from foo in foos
						  select new
						  {
							  Bar = foo.Bar,
							  Count = 1L
						  };

			Reduce = results => from result in results
								group result by result.Bar
									into g
									select new
									{
										Bar = g.Key,
										Count = g.Sum(c => c.Count)
									};
		}
	}

	class Program
	{
		static void Main(string[] args)
		{
			using (var store = new EmbeddableDocumentStore
			{
				DataDirectory = @"~\data",
				UseEmbeddedHttpServer = true
			}.Initialize())
			{

				var sp = Stopwatch.StartNew();
				for (int i = 0; i < 5000; i++)
				{
					using (var session = store.OpenSession())
					{
						for (int j = 0; j < 100; j++)
						{
							session.Store(new Foo { Bar = "IamBar" });
						}
						session.SaveChanges();
					}
					if(i % 100 == 0)
					{
						Console.Write(".");
					}
				}

				Console.Clear();
				Console.WriteLine("Wrote 500,000 docs in " + sp.Elapsed);
				Console.WriteLine("Done inserting data");

				sp.Restart();
				new SimpleMRIndex().Execute(store);
				Console.WriteLine("indexing...");

				while (store.DatabaseCommands.GetStatistics().StaleIndexes.Length > 0)
				{
					Console.Write("\r{0:#,#} - {1:#,#}",store.DatabaseCommands.GetStatistics().Indexes[0].IndexingAttempts, store.DatabaseCommands.GetStatistics().Indexes[0].ReduceIndexingAttempts);
					Thread.Sleep(100);
				}
				Console.WriteLine();
				Console.WriteLine("Indexed 500,000 docs in " + sp.Elapsed);

				Console.ReadLine();
			}
		}
	}
}