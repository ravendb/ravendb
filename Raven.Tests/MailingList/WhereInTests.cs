using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class WhereInTests : RavenTest
	{
		[Fact]
		public void WhereIn_using_index_analyzed()
		{
			using (IDocumentStore documentStore = NewDocumentStore())
			{
				new PersonsAnalyzed().Execute(documentStore);

				string[] names = {"Person One", "PersonTwo"};

				StoreObjects(new List<Person>
				{
					new Person {Name = names[0]},
					new Person {Name = names[1]}
				}, documentStore);

				using (var session = documentStore.OpenSession())
				{
					var query = session.Advanced.LuceneQuery<Person, PersonsAnalyzed>().WhereIn(p => p.Name, names);
					Assert.Equal(2, query.ToList().Count()); // Expected 2 but was 1 ("PersonTwo")
				}
			}
		}

		[Fact]
		public void WhereIn_not_using_index()
		{
			using (IDocumentStore documentStore = NewDocumentStore())
			{

				string[] names = {"Person One", "PersonTwo"};

				StoreObjects(new List<Person>
				{
					new Person {Name = names[0]},
					new Person {Name = names[1]}
				}, documentStore);

				using (var session = documentStore.OpenSession())
				{
					var query = session.Advanced.LuceneQuery<Person>().WhereIn(p => p.Name, names);
					Assert.Equal(2, query.ToList().Count()); // Success
				}
			}
		}

		private void StoreObjects<T>(IEnumerable<T> objects, IDocumentStore documentStore)
		{
			using (var session = documentStore.OpenSession())
			{
				foreach (var o in objects)
				{
					session.Store(o);
				}
				session.SaveChanges();
			}
			WaitForIndexing(documentStore);
		}
	}

	public class Person
	{
		public string Name { get; set; }
	}

	public class PersonsAnalyzed : AbstractIndexCreationTask<Person>
	{
		public PersonsAnalyzed()
		{
			Map = organizations => from o in organizations
								   select new { o.Name };

			Indexes.Add(x => x.Name, FieldIndexing.Analyzed);
		}
	}
}