using System;
using System.Linq;
using Raven.Client.Embedded;
using Raven.Client.Indexes;

namespace Raven.Tryouts
{
	public class Program
	{
		public static void Main()
		{
			using(var store = new EmbeddableDocumentStore
			{
				DataDirectory = "Data"
			}.Initialize())
			{
				new PopulationByState().Execute(store);

				using(var session = store.OpenSession())
				{
					for (int i = 0; i < 40; i++)
					{
						session.Store(new Person
						{
							State = i % 2 == 0 ? "TX" : "CA"
						});
					}
					session.SaveChanges();
				}

				using(var session = store.OpenSession())
				{
					var q = session.Query<Population, PopulationByState>()
						.Customize(x=>x.WaitForNonStaleResults())
						.ToList();

					foreach (var population in q)
					{
						Console.WriteLine(population);
					}
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
