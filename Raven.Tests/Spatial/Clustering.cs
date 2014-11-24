// -----------------------------------------------------------------------
//  <copyright file="Clustering.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;
using Raven.Client;

namespace Raven.Tests.Spatial
{
	public class Clustering : RavenTest
	{
		public class Location
		{
			public double Lng { get; set; }
			public double Lat { get; set; }
			public string Name { get; set; }
		}

		public class Location_Clustering : AbstractIndexCreationTask<Location>
		{
			public Location_Clustering()
			{
				Map = locations =>
				      from location in locations
				      select new
				      {
						_ = SpatialGenerate(location.Lat, location.Lng),
						__ = SpatialClustering("Cluster",location.Lat, location.Lng),
				      };
			}
		}

		[Fact]
		public void CanClusterData()
		{
			using (var store = NewDocumentStore())
			{
				new Location_Clustering().Execute(store);

				using (var s = store.OpenSession())
				{
					s.Store(new Location
					{
						Lat = 32.44611,
						Lng = 34.91098,
						Name = "Office"
					});

					s.Store(new Location
					{
						Lat = 32.43734,
						Lng = 34.92110,
						Name = "Mall"
					});


					s.Store(new Location
					{
						Lat = 32.43921,
						Lng = 34.90127,
						Name = "Rails"
					});

					s.SaveChanges();
				}

				WaitForIndexing(store);

				using (var s = store.OpenSession())
				{
					var results = s.Query<Location, Location_Clustering>()
					 .AggregateBy("Cluster_5")
					 .CountOn(x => x.Name)
					 .AndAggregateOn("Cluster_8")
					 .CountOn(x => x.Name)
					 .ToList();

					Assert.Equal(1, results.Results["Cluster_5"].Values.Count);
					Assert.Equal(3, results.Results["Cluster_8"].Values.Count);
				}
			}
		}
	}
}