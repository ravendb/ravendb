using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Linq.Indexing;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Maxime2 : RavenTest
	{
		[Fact]
		public void Spatial_Search_Should_Integrate_Distance_As_A_Boost_Factor()
		{
			using(var store = new EmbeddableDocumentStore { RunInMemory = true }.Initialize())
			{
				store.ExecuteIndex(new SpatialIndex());

				using (var session = store.OpenSession())
				{
					session.Store(new SpatialEntity(45.70955, -73.569131)
					{
						Id = "se/1",
						Name = "Universite du Quebec a Montreal",
						Description = "UQAM",
					});

					session.Store(new SpatialEntity(45.50955, -73.569131)
					{
						Id = "se/2",
						Name = "UQAM",
						Description = "Universite du Quebec a Montreal",
					});

					session.Store(new SpatialEntity(45.60955, -73.569131)
					{
						Id = "se/3",
						Name = "UQAM",
						Description = "Universite du Quebec a Montreal",
					});

					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var results = session.Advanced.LuceneQuery<SpatialEntity>("SpatialIndex")
						.Where("Name: UQAM OR Description: UQAM")
						.WithinRadiusOf(500, 45.50955, -73.569133)
						.ToList();

					Assert.True(results[0].Id == "se/2");
					Assert.True(results[1].Id == "se/3");
					Assert.True(results[2].Id == "se/1");
				}

			}
		}

		public class SpatialIndex : AbstractIndexCreationTask<SpatialEntity>
		{
			public SpatialIndex()
			{
				Map =
					entities =>
					from e in entities
					select new
					{
						Name = e.Name.Boost(3),
						e.Description,
						_ = SpatialGenerate(e.Latitude, e.Longitude)
					};

				Index(e => e.Name, FieldIndexing.Analyzed);
				Index(e => e.Description, FieldIndexing.Analyzed);
			}
		}

		public class SpatialEntity
		{
			public SpatialEntity() { }

			public SpatialEntity(double latitude, double longitude)
			{
				Latitude = latitude;
				Longitude = longitude;
			}

			public string Id { get; set; }
			public string Name { get; set; }
			public string Description { get; set; }
			public double Latitude { get; set; }
			public double Longitude { get; set; }
		}
	}
}