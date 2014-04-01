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

		[Fact]
		public void Can_project_Id_from_transformResults()
		{
			using (GetNewServer())
			using (var store = new DocumentStore {Url = "http://localhost:8079"})
			{
				store.Initialize();
				store.Conventions.FindIdentityProperty = (x => x.Name == "Id");
				var indexDefinition = new IndexDefinitionBuilder<Shipment1, Shipment1>()
				                      	{
				                      		Map = docs => from doc in docs
				                      		              select new
				                      		                     	{
				                      		                     		doc.Id
				                      		                     	},
				                      		TransformResults = (database, results) => from doc in results
				                      		                                          select new
				                      		                                                 	{
				                      		                                                 		Id = doc.Id,
				                      		                                                 		Name = doc.Name
				                      		                                                 	}

				                      	}.ToIndexDefinition(store.Conventions);
				store.DatabaseCommands.PutIndex(
					"AmazingIndex1",
					indexDefinition);


				using (var session = store.OpenSession())
				{
					session.Store(new Shipment1()
					              	{
					              		Id = "shipment1",
					              		Name = "Some shipment"
					              	});
					session.SaveChanges();

					var shipment = session.Query<Shipment1>("AmazingIndex1")
						.Customize(x => x.WaitForNonStaleResults())
						.Select(x => new Shipment1
						             	{
						             		Id = x.Id,
						             		Name = x.Name
						             	}).Take(1).SingleOrDefault();

					Assert.NotNull(shipment.Id);
				}
			}
		}

		public class Shipment2
		{
			public string InternalId { get; set; }
			public string Name { get; set; }
		}

		[Fact]
		public void Can_project_InternalId_from_transformResults()
		{
			using (GetNewServer())
			using (var store = new DocumentStore {Url = "http://localhost:8079"})
			{
				store.Initialize();
				store.Conventions.FindIdentityProperty = (x => x.Name == "InternalId");
				var indexDefinition = new IndexDefinitionBuilder<Shipment2, Shipment2>()
				                      	{
				                      		Map = docs => from doc in docs
				                      		              select new
				                      		                     	{
				                      		                     		doc.InternalId
				                      		                     	},
				                      		TransformResults = (database, results) => from doc in results
				                      		                                          select new
				                      		                                                 	{
				                      		                                                 		InternalId = doc.InternalId,
				                      		                                                 		Name = doc.Name
				                      		                                                 	}

				                      	}.ToIndexDefinition(store.Conventions);
				store.DatabaseCommands.PutIndex(
					"AmazingIndex2",
					indexDefinition);


				using (var session = store.OpenSession())
				{
					session.Store(new Shipment2()
					              	{
					              		InternalId = "shipment1",
					              		Name = "Some shipment"
					              	});
					session.SaveChanges();

					var shipment = session.Query<Shipment2>("AmazingIndex2")
						.Customize(x => x.WaitForNonStaleResults())
						.Select(x => new Shipment2
						             	{
						             		InternalId = x.InternalId,
						             		Name = x.Name
						             	}).Take(1).SingleOrDefault();

					Assert.NotNull(shipment.InternalId);
				}
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
																		TransformResults = "from item in results select new { DocId = item.__document_id, Name2 = item.Name, City = item.City };",
				                                                  		Fields = new List<string> {"DocId", "Name", "City"}
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

                    var item = session.Advanced.DocumentQuery<TestResultItem>("TestItemsIndex")
						.WaitForNonStaleResultsAsOfNow()
						.ToList().First();

					Assert.NotNull(item.DocId);
					Assert.NotNull(item.Name2);
				}
			}
		}
	}
}