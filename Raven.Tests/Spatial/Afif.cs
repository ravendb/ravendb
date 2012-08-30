using System;
using System.Collections.Generic;
using System.Linq;
using FizzWare.NBuilder;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.Spatial
{
	public class Afif
	{
		public class ByVehicle : AbstractIndexCreationTask<Vehicle>
		{
			public ByVehicle()
			{
				Map = vehicles => from vehicle in vehicles
								  select new
								  {
									  vehicle.Model,
									  vehicle.Make,
									  _ = SpatialGenerate(vehicle.Latitude, vehicle.Longitude)
								  };
			}
		}

		public class Vehicle
		{
			public string Id { get; set; }
			public string Model { get; set; }
			public string Make { get; set; }
			public double Latitude { get; set; }
			public double Longitude { get; set; }
		}

		public class HawthornEast : Location
		{
			public HawthornEast()
				: base(longitude: 145.052097, latitude: -37.834855)
			{ }
		}

		public class Darwin : Location
		{
			public Darwin()
				: base(longitude: 130.841904, latitude: 12.461334)
			{ }
		}

		public class Location
		{
			public Location(double longitude, double latitude)
			{
				Longitude = longitude;
				Latitude = latitude;
			}

			public double Latitude { get; private set; }
			public double Longitude { get; private set; }
		}

		public class CanGetFacetsOnVehicleSpatialSearch :UsingEmbeddedRavenStoreWithVehicles
		{
			[Fact]
			public void ShouldMatchMakeFacetsOnLocation()
			{
				FacetResults facetvalues;

				using (var s = Store.OpenSession())
				{
					var index = typeof(ByVehicle).Name;

					facetvalues = Store.DatabaseCommands.GetFacets(
						index: index,
						query: new SpatialIndexQuery()
						{
							QueryShape = SpatialIndexQuery.GetQueryShapeFromLatLon(new Darwin().Latitude, new Darwin().Longitude, 5),
							SpatialRelation = SpatialRelation.Within,
							SpatialFieldName = Constants.DefaultSpatialFieldName,
						},
						facetSetupDoc: "facets/Vehicle");
				}

				Assert.NotNull(facetvalues);
				Assert.Equal(2, facetvalues.Results["Make"].Values.Count());
			}
		}

		public abstract class UsingEmbeddedRavenStoreWithVehicles : UsingEmbeddedRavenStore
		{
			protected static IEnumerable<Vehicle> Vehicles { get; set; }

			public UsingEmbeddedRavenStoreWithVehicles()
			{
				Open();
				Vehicles = Builder<Vehicle>.CreateListOfSize(10)
					.TheFirst(3)
						.With(x => x.Make = "Mazda")
						.With(x => x.Model = "Rx8")
						.With(x => x.Latitude = new Darwin().Latitude)
						.With(x => x.Longitude = new Darwin().Longitude)
					.TheNext(3)
						.With(x => x.Make = "Mercedes")
						.With(x => x.Model = "AMG")
						.With(x => x.Latitude = new Darwin().Latitude)
						.With(x => x.Longitude = new Darwin().Longitude)
					.TheNext(4)
						.With(x => x.Make = "Toyota")
						.With(x => x.Model = "Camry")
						.With(x => x.Latitude = new HawthornEast().Latitude)
						.With(x => x.Longitude = new HawthornEast().Longitude)
					.Build();

				using (var session = Store.OpenSession())
				{
					session.Store(new FacetSetup
					{
						Id = "facets/Vehicle",
						Facets = new List<Facet> { new Facet { Name = "Make" }, new Facet { Name = "Model" } }
					});
					new ByVehicle().Execute(session.Advanced.DocumentStore);
					session.SaveChanges();

					foreach (var vehicle in Vehicles)
						session.Store(vehicle);
					session.SaveChanges();

					RavenQueryStatistics stats;
					session.Query<Vehicle, ByVehicle>().Where(x => x.Make == "Mazda").Customize(x => x.WaitForNonStaleResults()).
						Statistics(out stats).ToList();
				}
			}

			public override void Dispose()
			{
				Vehicles = null;
				base.Dispose();
			}
		}

		public abstract class UsingEmbeddedRavenStore : IDisposable
		{
			protected EmbeddableDocumentStore Store { get; set; }

			protected void Open()
			{
				Store = new EmbeddableDocumentStore
				{
					RunInMemory =
						true
				};
				Store.Initialize();
			}

			public virtual void Dispose()
			{
				Store.Dispose();
			}
		}
	}
}