using System.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class spokeypokey : RemoteClientTest
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
			using (var store = new DocumentStore { Url = "http://localhost:8080" })
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
			using(GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8080" })
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
	}
}