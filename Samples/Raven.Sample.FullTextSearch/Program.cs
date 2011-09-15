using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Linq;

namespace Raven.Sample.FullTextSearch
{
	class Program
	{
		static void Main(string[] args)
		{
			using(var store = new EmbeddableDocumentStore
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

					using(var session = store.OpenSession())
					{
						Console.WriteLine("Equality");
						foreach (var person in Queryable.Where(session.Query<Person, People_ByName_FullTextSearch>(), x => x.Name == name))
						{
							Console.WriteLine(person.Name);
						}

						Console.WriteLine();

						Console.WriteLine("Simple starts with");
						foreach (var person in Queryable.Where(session.Query<Person, People_ByName_FullTextSearch>(), x => x.Name.StartsWith(name)))
						{
							Console.WriteLine(person.Name);
						}

						Console.WriteLine("Complex starts with");

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
			using(var session = store.OpenSession())
			{
				session.Store(new Person{Name = "Ayende Rahien"});
				session.Store(new Person{Name = "Oren Eini"});
				session.Store(new Person{ Name = "Oscar Eini" });
				session.Store(new Person{ Name = "Arava Eini" });

				session.SaveChanges();
			}
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
			                select new {person.Name};
			Index(x=>x.Name, FieldIndexing.Analyzed);
		}
	}
}
