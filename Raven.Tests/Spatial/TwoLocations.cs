// -----------------------------------------------------------------------
//  <copyright file="TwoLocations.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Spatial
{
	public class TwoLocations : RavenTest
	{
		public class Event
		{
			public string Name;
			public Location[] Locations;

			public class Location
			{
				public double Lng, Lat;
			}
		}

		private static void Setup(IDocumentStore store)
		{
			using (var session = store.OpenSession())
			{
				session.Store(new Event
				{
					Name = "Trial",
					Locations = new[]
						{
							new Event.Location
							{
								Lat =32.1067536,
								Lng = 34.8357353	
							}, 
							new Event.Location
							{
								Lat = 32.0624912,
								Lng = 34.7700725	
							}, 
						}
				});
				session.SaveChanges();
			}
		}

		[Fact]
		public void CanQueryByMultipleLocations()
		{
			using (var store = NewDocumentStore())
			{
				new MultiLocations().Execute(store);
				Setup(store);

				using (var session = store.OpenSession())
				{
					var list = session.Query<Event, MultiLocations>()
						.Customize(x => x.WaitForNonStaleResults())
						.Customize(x => x.WithinRadiusOf(1, 32.0590291, 34.7707401))
						.ToList();

					Assert.Empty(store.DocumentDatabase.Statistics.Errors);

					Assert.NotEmpty(list);
				}

				using (var session = store.OpenSession())
				{
					var list = session.Query<Event, MultiLocations>()
						.Customize(x => x.WaitForNonStaleResults())
						.Customize(x => x.WithinRadiusOf(1, 32.1104641, 34.8417456))
						.ToList();

					Assert.Empty(store.DocumentDatabase.Statistics.Errors);

					Assert.NotEmpty(list);
				}
			}
		}

		[Fact]
		public void CanQueryByMultipleLocations2()
		{
			using (var store = NewDocumentStore())
			{
				new MultiLocationsCustomFieldName().Execute(store);
				Setup(store);

				using (var session = store.OpenSession())
				{
					var list = session.Query<Event, MultiLocationsCustomFieldName>()
						.Customize(x => x.WaitForNonStaleResults())
						.Customize(x => x.WithinRadiusOf("someField", 1, 32.0590291, 34.7707401))
						.ToList();

					Assert.Empty(store.DocumentDatabase.Statistics.Errors);

					Assert.NotEmpty(list);
				}

				using (var session = store.OpenSession())
				{
					var list = session.Query<Event, MultiLocationsCustomFieldName>()
						.Customize(x => x.WaitForNonStaleResults())
						.Customize(x => x.WithinRadiusOf("someField", 1, 32.1104641, 34.8417456))
						.ToList();

					Assert.Empty(store.DocumentDatabase.Statistics.Errors);

					Assert.NotEmpty(list);
				}
			}
		}

		[Fact]
		public void CanQueryByMultipleLocationsOverHttp()
		{
			using (var store = NewRemoteDocumentStore())
			{
				new MultiLocations().Execute(store);
				Setup(store);

				using (var session = store.OpenSession())
				{
					var list = session.Query<Event, MultiLocations>()
						.Customize(x => x.WaitForNonStaleResults())
						.Customize(x => x.WithinRadiusOf(1, 32.0590291, 34.7707401))
						.ToList();

					Assert.NotEmpty(list);
				}

				using (var session = store.OpenSession())
				{
					var list = session.Query<Event, MultiLocations>()
						.Customize(x => x.WaitForNonStaleResults())
						.Customize(x => x.WithinRadiusOf(1, 32.1104641, 34.8417456))
						.ToList();

					Assert.NotEmpty(list);
				}
			}
		}

		[Fact]
		public void CanQueryByMultipleLocationsHttp2()
		{
			using (var store = NewRemoteDocumentStore())
			{
				new MultiLocationsCustomFieldName().Execute(store);
				Setup(store);

				using (var session = store.OpenSession())
				{
					var list = session.Query<Event, MultiLocationsCustomFieldName>()
						.Customize(x => x.WaitForNonStaleResults())
						.Customize(x => x.WithinRadiusOf("someField", 1, 32.0590291, 34.7707401))
						.ToList();

					Assert.NotEmpty(list);
				}

				using (var session = store.OpenSession())
				{
					var list = session.Query<Event, MultiLocationsCustomFieldName>()
						.Customize(x => x.WaitForNonStaleResults())
						.Customize(x => x.WithinRadiusOf("someField", 1, 32.1104641, 34.8417456))
						.ToList();

					Assert.NotEmpty(list);
				}
			}
		}

		[Fact]
		public void CanQueryByMultipleLocationsRaw()
		{
			using (var store = NewDocumentStore())
			{
				new MultiLocationsCustomFieldName().Execute(store);
				Setup(store);

				using (var session = store.OpenSession())
				{
					var list = session.Query<Event, MultiLocationsCustomFieldName>()
						.Customize(x => x.WaitForNonStaleResults())
						.Customize(x => x.RelatesToShape("someField", "Circle(34.770740 32.059029 d=1.000000)", SpatialRelation.Within))
						.ToList();

					Assert.Empty(store.DocumentDatabase.Statistics.Errors);

					Assert.NotEmpty(list);
				}

				using (var session = store.OpenSession())
				{
					var list = session.Query<Event, MultiLocationsCustomFieldName>()
						.Customize(x => x.WaitForNonStaleResults())
						.Customize(x => x.RelatesToShape("someField", "Circle(34.770740 32.059029 d=1.000000)", SpatialRelation.Within))
						.ToList();

					Assert.Empty(store.DocumentDatabase.Statistics.Errors);

					Assert.NotEmpty(list);
				}
			}
		}

		[Fact]
		public void CanQueryByMultipleLocationsRawOverHttp()
		{
			using (var store = NewRemoteDocumentStore())
			{
				new MultiLocationsCustomFieldName().Execute(store);
				Setup(store);

				using (var session = store.OpenSession())
				{
					var list = session.Query<Event, MultiLocationsCustomFieldName>()
						.Customize(x => x.WaitForNonStaleResults())
						.Customize(x => x.RelatesToShape("someField", "Circle(34.770740 32.059029 d=1.000000)", SpatialRelation.Within))
						.ToList();

					Assert.NotEmpty(list);
				}

				using (var session = store.OpenSession())
				{
					var list = session.Query<Event, MultiLocationsCustomFieldName>()
						.Customize(x => x.WaitForNonStaleResults())
						.Customize(x => x.RelatesToShape("someField", "Circle(34.770740 32.059029 d=1.000000)", SpatialRelation.Within))
						.ToList();

					Assert.NotEmpty(list);
				}
			}
		}

		public class MultiLocations : AbstractIndexCreationTask<Event>
		{
			public MultiLocations()
			{
				Map = events =>
				      from e in events
				      select new
				      {
				      	e.Name,
				      	_ = e.Locations.Select(x => SpatialGenerate(x.Lat, x.Lng))
				      };
			}
		}

		public class MultiLocationsCustomFieldName : AbstractIndexCreationTask<Event>
		{
			public MultiLocationsCustomFieldName()
			{
				Map = events =>
					  from e in events
					  select new
					  {
						  e.Name,
						  _ = e.Locations.Select(x => SpatialGenerate("someField", x.Lat, x.Lng))
					  };
			}
		}
	}
}