// -----------------------------------------------------------------------
//  <copyright file="SupportForJsonPropertyAttribute.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;
using Raven.Imports.Newtonsoft.Json;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Json
{
	public class SupportForJsonPropertyAttribute : RavenTest
	{
		public class Station
		{
			[JsonProperty("name")]
			public string Name { get; set; }

			[JsonProperty("lat")]
			public double Latitude { get; set; }

			[JsonProperty("long")]
			public double Longitude { get; set; }
		}

		public class Stations_ByLocation : AbstractIndexCreationTask<Station>
		{
			public Stations_ByLocation()
			{
				Map = stations =>
						from s in stations
						select new { _ = SpatialIndex.Generate(s.Latitude, s.Longitude) };
			}
		}

		[Fact]
		public void ShouldHaveTheCorrectIndex()
		{
			using (var store = NewDocumentStore())
			{
				new Stations_ByLocation().Execute(store);
				var definition = store.DatabaseCommands.GetIndex(new Stations_ByLocation().IndexName);

				Assert.Equal(@"docs.Stations.Select(s => new {
    _ = SpatialIndex.Generate(((double ? ) s.lat), ((double ? ) s.@long))
})", definition.Map);



				Assert.NotEqual(@"docs.Stations.Select(s => new {
    _ = SpatialIndex.Generate(((double ? )  s.Latitude), ((double ? ) s.Longitude))
})", definition.Map);
			}
		}
	}
}