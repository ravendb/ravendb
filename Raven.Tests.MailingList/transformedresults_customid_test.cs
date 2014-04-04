using System.Collections.Generic;
using System.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class transformedresults_customid_test : RavenTest
	{
		public class TestItem
		{
			public string DocId { get; set; }
			public string Name { get; set; }
			public string City { get; set; }
		}

		public class TestResultItem
		{
			public string DocId { get; set; }
			public string Name { get; set; }
		}


		[Fact]
		public void Can_project_InternalId_from_transformResults()
		{
			using(GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" })
			{
				store.Initialize();
				store.Conventions.FindIdentityProperty = (x => x.Name == "DocId");
				store.DatabaseCommands.PutIndex("TestItemsIndex", new IndexDefinitionBuilder<TestItem, TestItem>
				{
					Map = items => from item in items select new { DocId = item.DocId, Name = item.Name, City = item.City },
					TransformResults = (db, results) => from item in results 
														select new { DocId = item.DocId, Name = item.Name },
				}, true);

				using (var session = store.OpenSession())
				{
					session.Store(new TestItem()
					{
						DocId = "testitems/500",
						Name = "My first item",
						City = "New york"
					});
					session.Store(new TestItem()
					{
						DocId = "testitems/501",
						Name = "My second item",
						City = "London"
					});
					session.SaveChanges();

					TestResultItem item = session
                       .Advanced.DocumentQuery<TestResultItem>("TestItemsIndex")
					   .WaitForNonStaleResults()
					   .ToList().First();

					Assert.NotNull(item.DocId);
				}
			}
		}
	}
}