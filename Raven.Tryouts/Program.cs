using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database.Extensions;

namespace Raven.Tryouts
{
	public class Program
	{
		public static void Main()
		{
			using(var store = new EmbeddableDocumentStore
			{
				RunInMemory = true,
				UseEmbeddedHttpServer = true
			}.Initialize())
			{
				new PopulationByState().Execute(store);

				for (int aa = 0; aa < 1000; aa++)
				{
					using (var session = store.OpenSession())
					{
						for (int i = 0; i < 5; i++)
						{
							session.Store(new Person
							{
								State = i % 2 == 0 ? "TX" : "CA"
							});
						}
						session.SaveChanges();
					}

					var sp = Stopwatch.StartNew();
					using (var session = store.OpenSession())
					{
						var q = session.Query<Population, PopulationByState>()
							.Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(10)))
							.ToList();

						foreach (var population in q)
						{
							Console.WriteLine(population);
						}
					}
					Console.WriteLine("Reduced in {0:#,#} ms",sp.ElapsedMilliseconds);

					Console.WriteLine("Press key to continue");
					Console.ReadKey();
				}
			}

		}

		public class Person
		{
			public string State { get; set; }
		}
		public class Population
		{
			public int Count { get; set; }
			public string State { get; set; }

			public override string ToString()
			{
				return string.Format("Count: {0}, State: {1}", Count, State);
			}
		}
		public class PopulationByState : AbstractIndexCreationTask<Person, Population>
		{
			public PopulationByState()
			{
				Map = people => from person in people
				                select new { person.State, Count = 1 };
				Reduce = results => from result in results
				                    group result by result.State
				                    into g
				                    select new { State = g.Key, Count = g.Sum(x => x.Count) };
			}
		}
	}
}
