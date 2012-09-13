using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Client.Linq.Indexing;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Maxime : RavenTest
	{
		[Fact]
		public void WithingRadiusOf_Should_Not_Break_Relevance()
		{
			using (var store = NewDocumentStore())
			using (var session = store.OpenSession())
			{
				new PlacesByTermsAndLocation().Execute(store);

				var place1 = new Place("Université du Québec à Montréal")
				{
					Id = "places/1",
					Description = "L'Université du Québec à Montréal (UQAM) est une université francophone, publique et urbaine de Montréal, dans la province du Québec au Canada.",
					Latitude = 45.50955,
					Longitude = -73.569131
				};

				var place2 = new Place("UQAM")
				{
					Id = "places/2",
					Description = "L'Université du Québec à Montréal (UQAM) est une université francophone, publique et urbaine de Montréal, dans la province du Québec au Canada.",
					Latitude = 45.50955,
					Longitude = -73.569131
				};

				session.Store(place1);
				session.Store(place2);

				session.SaveChanges();

				// places/2: perfect match + boost
				var terms = "UQAM";
				RavenQueryStatistics stats;
				var places = session.Advanced.LuceneQuery<Place, PlacesByTermsAndLocation>()
					.WaitForNonStaleResults()
					.Statistics(out stats)
					.WithinRadiusOf(500, 45.54545, -73.63908)
					.Where("(Name:(" + terms + ") OR Terms:(" + terms + "))")
					.Take(10)
					.ToList();

				Assert.Equal("places/2", places[0].Id);
				// places/1: perfect match + boost
				terms = "Université Québec Montréal";
				places = session.Advanced.LuceneQuery<Place, PlacesByTermsAndLocation>()
					.WaitForNonStaleResults()
					.Statistics(out stats)
					.WithinRadiusOf(500, 45.54545, -73.63908)
					.Where("(Name:(" + terms + ") OR Terms:(" + terms + "))")
					.Take(10)
					.ToList();

				Assert.Equal("places/1", places[0].Id);
			}
		}


		[Fact]
		public void Can_just_set_to_sort_by_relevance_without_filtering()
		{
			using (var store = NewDocumentStore())
			using (var session = store.OpenSession())
			{
				new PlacesByTermsAndLocation().Execute(store);

				var place1 = new Place("Université du Québec à Montréal")
				{
					Id = "places/1",
					Description = "L'Université du Québec à Montréal (UQAM) est une université francophone, publique et urbaine de Montréal, dans la province du Québec au Canada.",
					Latitude = 45.50955,
					Longitude = -73.569131
				};

				var place2 = new Place("UQAM")
				{
					Id = "places/2",
					Description = "L'Université du Québec à Montréal (UQAM) est une université francophone, publique et urbaine de Montréal, dans la province du Québec au Canada.",
					Latitude = 45.50955,
					Longitude = -73.569131
				};

				session.Store(place1);
				session.Store(place2);

				session.SaveChanges();

				// places/1: perfect match + boost
				const string terms = "Université Québec Montréal";
				RavenQueryStatistics stats;
				var places = session.Advanced.LuceneQuery<Place, PlacesByTermsAndLocation>()
					.WaitForNonStaleResults()
					.Statistics(out stats)
					.RelatesToShape(Constants.DefaultSpatialFieldName, "Point(45.54545 -73.63908)", SpatialRelation.Nearby)
					.Where("(Name:(" + terms + ") OR Terms:(" + terms + "))")
					.Take(10)
					.ToList();

				Assert.Equal("places/1", places[0].Id);
				Assert.Equal("places/2", places[1].Id);
			}
		}

		public class Place
		{
			public Place(string name)
			{
				Name = name;
			}

			public string Id { get; set; }
			public string Name { get; set; }
			public string Description { get; set; }
			public string Address { get; set; }
			public double Latitude { get; set; }
			public double Longitude { get; set; }
		}

		public class PlacesByTermsAndLocation : AbstractIndexCreationTask<Place, PlacesByTermsAndLocation.PlaceQuery>
		{
			public class PlaceQuery
			{
				public string Name { get; set; }
				public string Terms { get; set; }
			}

			public PlacesByTermsAndLocation()
			{
				Map = boards =>
					  from b in boards
					  select new
					  {
						  Name = b.Name.Boost(3),
						  Terms = new
						  {
							  b.Description,
							  b.Address
						  },
						  _ = SpatialGenerate(b.Latitude, b.Longitude)
					  };

				Index(p => p.Name, FieldIndexing.Analyzed);
				Index(p => p.Terms, FieldIndexing.Analyzed);

			}
		} 
	}
}