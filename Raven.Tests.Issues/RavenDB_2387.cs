using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2387 : RavenTestBase
	{
		public class ParentDoc
		{
			public string Data { get; set; }

			public string ChildDocId { get; set; }
		}

		public class ChildDoc
		{
			public string Id { get; set; }

			public string Data { get; set; }
		}

		public class ChildWithParentDocument : AbstractIndexCreationTask<ParentDoc>
		{
			public ChildWithParentDocument()
			{
				Map = docs => from parentDoc in docs
							  select new
							  {
								  LoadDocument<ChildDoc>(parentDoc.ChildDocId).Data
							  };
			}
		}

		[Fact]
		public void Updates_within_dtc_should_be_reindexed_correctly()
		{
			using (var store = NewDocumentStore(requestedStorage: "esent"))
			{
				new ChildWithParentDocument().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new ParentDoc
					{
						Data = "Foo",
						ChildDocId = "child/2"
					});

					session.Store(new ChildDoc
					{
						Id = "child/2",
						Data = "Foo"
					});

					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var objects = session.Advanced.DocumentQuery<object>("ChildWithParentDocument").WhereEquals("Data", "Foo").ToList();
					Assert.Equal(1, objects.Count);
				}

				var tx = new TransactionInformation
				{
					Id = "tx",
					Timeout = TimeSpan.FromDays(1)
				};
				store.SystemDatabase.Documents.Put("child/2", null, new RavenJObject { { "Data", "Bar" } }, new RavenJObject(), tx);

				store.SystemDatabase.PrepareTransaction("tx");

				using (var session = store.OpenSession())
				{

					session.Store(new ChildDoc
					{
						Id = "child/1",
						Data = "Foo"
					});

					session.SaveChanges();
				}

				WaitForIndexing(store);

				store.SystemDatabase.Commit("tx");

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var objects = session.Advanced.DocumentQuery<object>("ChildWithParentDocument").WhereEquals("Data", "Bar").ToList();
					Assert.Equal(1, objects.Count);
				}


			}
		}
	}
}
