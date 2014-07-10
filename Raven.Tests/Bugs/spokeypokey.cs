using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;
using System.Collections.Generic;

namespace Raven.Tests.Bugs
{
	public class spokeypokey : RavenTest
	{
		public class Shipment1
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class Shipment2
		{
			public string InternalId { get; set; }
			public string Name { get; set; }
		}

		public class TransformerWithInternalId : AbstractTransformerCreationTask<Shipment2>
		{
			public TransformerWithInternalId()
			{
				TransformResults = docs => from doc in docs
											 select new
											 {
												 InternalId = doc.InternalId,
												 Name = doc.Name
											 };
			}
		}

		public class TestItem
		{
			public string DocId { get; set; }
			public string Name { get; set; }
			public string City { get; set; }
		}

		public class TestResultItem
		{
			public string DocId { get; set; }
			public string Name2 { get; set; }
		}

		[Fact]
		public void Can_project_InternalId_from_transformResults2()
		{
			using (var store = NewDocumentStore())
			{
				store.Initialize();
				store.Conventions.FindIdentityProperty = (x => x.Name == "DocId");
				store.DatabaseCommands.PutIndex("TestItemsIndex", new IndexDefinition
				                                                  	{
				                                                  		Name = "TestItemsIndex",
				                                                  		Map = "from item in docs.TestItems select new { DocId = item.__document_id, Name2 = item.Name, City = item.City };",
				                                                  		Fields = new List<string> {"DocId", "Name", "City"}
				                                                  	}, true);

				var transformerName = "TransformerWithInternalIds";

				store.DatabaseCommands.PutTransformer(transformerName, new TransformerDefinition()
				{
					Name = transformerName,
					TransformResults = "from item in results select new { DocId = item.__document_id, Name2 = item.Name, City = item.City };"
				});

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

                    var item = session.Advanced.DocumentQuery<TestResultItem>("TestItemsIndex").SetResultTransformer(transformerName)
						.WaitForNonStaleResultsAsOfNow()
						.ToList().First();

					Assert.NotNull(item.DocId);
					Assert.NotNull(item.Name2);
				}
			}
		}
	}
}