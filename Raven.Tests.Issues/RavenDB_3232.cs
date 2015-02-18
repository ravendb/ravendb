// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3232.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3232 : RavenTest
	{
		private class Person
		{
			public string Id { get; set; }

			public string FirstName { get; set; }

			public string LastName { get; set; }
		}

		private class TestIndex : AbstractIndexCreationTask<Person>
		{
			public TestIndex()
			{
				Map = persons => from person in persons select new { person.FirstName, person.LastName };
			}
		}
		[Fact]
		public void ShouldSimplyCreateIndex()
		{
			using (var store = NewDocumentStore(runInMemory: false))
			{
				// since index dones't exists just it should simply create it instead of using side-by-side.
				new TestIndex().SideBySideExecute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new Person { FirstName = "John", LastName = "Doe" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var count = session.Query<Person, TestIndex>()
						.Count(x => x.LastName == "Doe");
				}
			}
		}

		[Fact]
		public void ReplaceOfNonStaleIndex()
		{
			using (var store = NewDocumentStore(runInMemory: false))
			{

				var oldIndexDef = new IndexDefinition
				{
					Map = "from person in docs.People\nselect new {\n\tFirstName = person.FirstName\n}"
				};
				store.DatabaseCommands.PutIndex("TestIndex", oldIndexDef);

				using (var session = store.OpenSession())
				{
					session.Store(new Person { FirstName = "John", LastName = "Doe" });
					session.SaveChanges();
				}

				WaitForIndexing(store);
				store.DocumentDatabase.StopBackgroundWorkers();

				new TestIndex().SideBySideExecute(store);

				var e = Assert.Throws<InvalidOperationException>(() =>
				{
					using (var session = store.OpenSession())
					{
						var count = session.Query<Person, TestIndex>()
							.Count(x => x.LastName == "Doe");
					}
				});

				Assert.Contains("The field 'LastName' is not indexed, cannot query on fields that are not indexed", e.InnerException.Message);

				store.DocumentDatabase.SpinBackgroundWorkers();

				WaitForIndexing(store);

				store
					.DatabaseCommands
					.Admin
					.StopIndexing();

				store.SystemDatabase.IndexReplacer.ReplaceIndexes(store.SystemDatabase.IndexStorage.Indexes);

				using (var session = store.OpenSession())
				{
					var count = session.Query<Person, TestIndex>()
						.Count(x => x.LastName == "Doe");

					Assert.Equal(1, count);
				}
			}
		}

		[Fact]
		public async Task ReplaceOfNonStaleIndexAsync()
		{
			using (var store = NewDocumentStore(runInMemory: false))
			{

				var oldIndexDef = new IndexDefinition
				{
					Map = "from person in docs.People\nselect new {\n\tFirstName = person.FirstName\n}"
				};
				await store.AsyncDatabaseCommands.PutIndexAsync("TestIndex", oldIndexDef).ConfigureAwait(false);

				using (var session = store.OpenSession())
				{
					session.Store(new Person { FirstName = "John", LastName = "Doe" });
					session.SaveChanges();
				}

				WaitForIndexing(store);
				store.DocumentDatabase.StopBackgroundWorkers();

				await new TestIndex().SideBySideExecuteAsync(store).ConfigureAwait(false);

				var e = Assert.Throws<InvalidOperationException>(() =>
				{
					using (var session = store.OpenSession())
					{
						var count = session.Query<Person, TestIndex>()
							.Count(x => x.LastName == "Doe");
					}
				});

				Assert.Contains("The field 'LastName' is not indexed, cannot query on fields that are not indexed", e.InnerException.Message);

				store.DocumentDatabase.SpinBackgroundWorkers();

				WaitForIndexing(store);

				store
					.DatabaseCommands
					.Admin
					.StopIndexing();

				store.SystemDatabase.IndexReplacer.ReplaceIndexes(store.SystemDatabase.IndexStorage.Indexes);

				using (var session = store.OpenSession())
				{
					var count = session.Query<Person, TestIndex>()
						.Count(x => x.LastName == "Doe");

					Assert.Equal(1, count);
				}
			}
		}
	}
}