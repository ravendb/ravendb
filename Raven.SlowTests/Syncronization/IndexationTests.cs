using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Synchronization
{
	public class IndexationTests : RavenTest
	{
		private class Person
		{
			public string FirstName { get; set; }

			public string LastName { get; set; }
		}

		private class PersonCount : AbstractIndexCreationTask<Person, PersonCount.ReduceResult>
		{
			public class ReduceResult
			{
				public string FirstName { get; set; }
				public string LastName { get; set; }
				public int Count { get; set; }
			}

			public PersonCount()
			{
				Map = people => from person in people
								select new
								{
									person.FirstName,
									person.LastName,
									Count = 1
								};

				Reduce = results => from result in results
									group result by new { result.FirstName, result.LastName }
										into g
										select new
										{
											FirstName = g.Key.FirstName,
											LastName = g.Key.LastName,
											Count = g.Sum(x => x.Count)
										};

			}
		}

		[Fact]
		public void IndexerTest()
		{
			using (var store = NewDocumentStore(requestedStorage:"esent"))
			{
				var index = new RavenDocumentsByEntityName();
				index.Execute(store);

				var tasks = new List<Task>();
				for (var i = 1; i <= 20; i++)
				{
					var taskNumber = i;
					tasks.Add(Save(store, taskNumber));
				}

				Task.WaitAll(tasks.ToArray());

				WaitForIndexing(store);

				Assert.Equal(20000, store.DatabaseCommands.GetStatistics().CountOfDocuments); 

				using (var session = store.OpenSession())
				{
					var count = session.Query<Person>(index.IndexName)
									   .Customize(x => x.WaitForNonStaleResults())
									   .Count();

					Assert.Equal(20000, count);
				}
			}
		}

		[Fact]
		public void ReducerTest()
		{
			using (var store = NewDocumentStore(requestedStorage:"esent"))
			{
				var index1 = new RavenDocumentsByEntityName();
				index1.Execute(store);
				var index2 = new PersonCount();
				index2.Execute(store);

				var tasks = new List<Task>();
				for (var i = 1; i <= 20; i++)
				{
					var taskNumber = i;
					tasks.Add(Save(store, taskNumber));
				}

				Task.WaitAll(tasks.ToArray());

				WaitForIndexing(store, timeout: TimeSpan.FromMinutes(1));

				Assert.Equal(20000, store.DatabaseCommands.GetStatistics().CountOfDocuments);

				using (var session = store.OpenSession())
				{
					var count = session.Query<Person>(index1.IndexName)
									   .Customize(x => x.WaitForNonStaleResults())
									   .Count();

					Assert.Equal(20000, count);

					var results = session.Query<PersonCount.ReduceResult, PersonCount>()
										 .Customize(customization => customization.WaitForNonStaleResults())
										 .Take(1001)
										 .ToList();


					WaitForUserToContinueTheTest(store);

					Assert.Equal(1000, results.Count);

					foreach (var result in results)
					{
						Assert.Equal(20, result.Count);
					}
				}
			}
		}

		private async Task Save(IDocumentStore store, int taskNumber)
		{
			for (var i = 1; i <= 1000; i++)
			{
				var response =
					await
					store.AsyncDatabaseCommands.PutAsync(
						string.Format("people/{0}", Guid.NewGuid()), null,
						RavenJObject.FromObject(new Person
						{
							FirstName = "FirstName" + i,
							LastName = "LastName" + i
						}), new RavenJObject
						{
							{Constants.RavenEntityName, "People"}
						});
			}
		}
	}
}
