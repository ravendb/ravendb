// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2867.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Threading;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2867 : RavenTest
	{
		private class Person
		{
			public string Id { get; set; }

			public string FirstName { get; set; }

			public string LastName { get; set; }
		}

		private class OldIndex : AbstractIndexCreationTask<Person>
		{
			public OldIndex()
			{
				Map = persons => from person in persons select new { person.FirstName };
			}
		}

		private class NewIndex : AbstractIndexCreationTask<Person>
		{
			public NewIndex()
			{
				Map = persons => from person in persons select new { person.FirstName, person.LastName };
			}
		}

        [Fact]
        public void DeleteReplacementDocumentShouldDeleteIndexAsWell()
        {
            using (var store = NewDocumentStore(runInMemory: false))
            {

                new OldIndex().Execute(store);
                new NewIndex().Execute(store);

                store
                    .DatabaseCommands
                    .Admin
                    .StopIndexing();

                store
                    .DatabaseCommands
                    .Put(Constants.IndexReplacePrefix + new NewIndex().IndexName, null, RavenJObject.FromObject(new IndexReplaceDocument { IndexToReplace = new OldIndex().IndexName, MinimumEtagBeforeReplace = null }), new RavenJObject());

				store.DatabaseCommands.Delete(Constants.IndexReplacePrefix + new NewIndex().IndexName, null);

                Assert.Null(store.DatabaseCommands.GetIndex(new NewIndex().IndexName));
            }
        }

        [Fact]
        public void DeleteSideBySideIndexShouldDeleteDocumentAsWell()
        {
            using (var store = NewDocumentStore(runInMemory: false))
            {

                new OldIndex().Execute(store);
                new NewIndex().Execute(store);

                store
                    .DatabaseCommands
                    .Admin
                    .StopIndexing();

                store
                    .DatabaseCommands
					.Put(Constants.IndexReplacePrefix + new NewIndex().IndexName, null, RavenJObject.FromObject(new IndexReplaceDocument { IndexToReplace = new OldIndex().IndexName, MinimumEtagBeforeReplace = null }), new RavenJObject());


                store.DatabaseCommands.DeleteIndex(new NewIndex().IndexName);

				Assert.Null(store.DatabaseCommands.Get(Constants.IndexReplacePrefix + new NewIndex().IndexName));
            }
        }

		[Fact]
		public void ReplaceOfNonStaleIndex()
		{
			using (var store = NewDocumentStore(runInMemory: false))
			{
				new OldIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new Person { FirstName = "John", LastName = "Doe" });
					session.SaveChanges();
				}

				WaitForIndexing(store);
				store.DocumentDatabase.StopBackgroundWorkers();

				new NewIndex().Execute(store);


				store
					.DatabaseCommands
					.Put(Constants.IndexReplacePrefix + new NewIndex().IndexName, null, 
					RavenJObject.FromObject(new IndexReplaceDocument { IndexToReplace = new OldIndex().IndexName, MinimumEtagBeforeReplace = null }), new RavenJObject());

				var e = Assert.Throws<InvalidOperationException>(() =>
				{
					using (var session = store.OpenSession())
					{
						var count = session.Query<Person, OldIndex>()
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
					var count = session.Query<Person, OldIndex>()
						.Count(x => x.LastName == "Doe");

					Assert.Equal(1, count);
				}
			}
		}

		[Fact]
		public void ReplaceAfterSomeTime()
		{
			using (var store = NewDocumentStore(runInMemory: false))
			{
				new OldIndex().Execute(store);
				new NewIndex().Execute(store);

				store
					.DatabaseCommands
					.Admin
					.StopIndexing();

				store
					.DatabaseCommands
					.Put(Constants.IndexReplacePrefix + new NewIndex().IndexName, null, RavenJObject.FromObject(new IndexReplaceDocument { IndexToReplace = new OldIndex().IndexName, ReplaceTimeUtc = SystemTime.UtcNow.AddMinutes(10) }), new RavenJObject());

				var e = Assert.Throws<InvalidOperationException>(() =>
				{
					using (var session = store.OpenSession())
					{
						var count = session.Query<Person, OldIndex>()
							.Count(x => x.LastName == "Doe");
					}
				});

				Assert.Contains("The field 'LastName' is not indexed, cannot query on fields that are not indexed", e.InnerException.Message);

				SystemTime.UtcDateTime = () => DateTime.UtcNow.AddMinutes(11);

				store.SystemDatabase.IndexReplacer.ReplaceIndexes(store.SystemDatabase.IndexStorage.Indexes);

				using (var session = store.OpenSession())
				{
					var count = session.Query<Person, OldIndex>()
						.Count(x => x.LastName == "Doe");
				}
			}
		}
	}
}