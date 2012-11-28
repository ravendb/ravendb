namespace Raven.Sample.FullTextSearch
{
	using System;
	using System.Collections.Generic;
	using System.Linq;

	using Raven.Abstractions.Indexing;
	using Raven.Client;
	using Raven.Client.Embedded;
	using Raven.Client.Indexes;
	using Raven.Client.Linq;

	public class Program
	{
		private static readonly IList<string> People = new List<string> { "Ayende Rahien", "Oren Eini", "Oscar Eini", "Arava Eini" };

		public static void Main(string[] args)
		{
			using (var store = new EmbeddableDocumentStore
			{
				RunInMemory = true,
				UseEmbeddedHttpServer = true
			}.Initialize())
			{
				IndexCreation.CreateIndexes(typeof(Program).Assembly, store);
				StoreDocs(store);

				while (true)
				{
					Console.Write("Enter name: ");
					var name = Console.ReadLine();

					using (var session = store.OpenSession())
					{
						Console.WriteLine("Equality");
						foreach (var person in session.Query<Person, People_ByName_FullTextSearch>().Where(x => x.Name == name))
						{
							Console.WriteLine(person.Name);
						}

						Console.WriteLine();

						Console.WriteLine("Simple starts with:");
						foreach (var person in session.Query<Person, People_ByName_FullTextSearch>().Where(x => x.Name.StartsWith(name)))
						{
							Console.WriteLine(person.Name);
						}

						Console.WriteLine("Complex starts with:");

						IQueryable<Person> query = session.Query<Person, People_ByName_FullTextSearch>();

						query = name.Split().Aggregate(query, (current, part) => current.Where(x => x.Name.StartsWith(part)));

						foreach (var person in query)
						{
							Console.WriteLine(person.Name);
						}
					}
				}
			}
		}

		private static void StoreDocs(IDocumentStore store)
		{
			using (var session = store.OpenSession())
			{
				foreach (var person in People)
				{
					session.Store(new Person { Name = person });
				}

				session.SaveChanges();
			}

			Console.WriteLine("Stored:");
			foreach (var person in People)
			{
				Console.WriteLine("{0}", person);
			}

			Console.WriteLine();
		}
	}

	public class Person
	{
		public string Name { get; set; }
	}

	public class People_ByName_FullTextSearch : AbstractIndexCreationTask<Person>
	{
		public People_ByName_FullTextSearch()
		{
			Map = people => from person in people
							select new { person.Name };
			Index(x => x.Name, FieldIndexing.Analyzed);
		}
	}
}
