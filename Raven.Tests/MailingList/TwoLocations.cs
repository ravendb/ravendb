// -----------------------------------------------------------------------
//  <copyright file="TwoLocations.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
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

		[Fact]
		public void CanQueryByMultipleLocations()
		{
			using (var store = NewDocumentStore())
			{
				new MultiLocations().Execute(store);
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

		public class MultiLocations : AbstractIndexCreationTask<Event>
		{
			public MultiLocations()
			{
				Map = events =>
				      from e in events
				      select new
				      {
				      	e.Name,
				      	_ = e.Locations.Select(x => SpatialIndex.Generate(x.Lat, x.Lng))
				      };
			}
		}
	}
}