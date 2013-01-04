//-----------------------------------------------------------------------
// <copyright file="SpatialIndexTestHelper.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.IO;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;

namespace Raven.Tests.Spatial
{
	public static class SpatialIndexTestHelper
	{
		public static Event[] GetEvents()
		{
			return new Event[]
			{
				new Event("McCormick &amp, Schmick's Seafood Restaurant", 38.9579000, -77.3572000),
				new Event("Jimmy's Old Town Tavern", 38.9690000, -77.3862000),
				new Event("Ned Devine's", 38.9510000, -77.4107000),
				new Event("Old Brogue Irish Pub", 38.9955000, -77.2884000),
				new Event("Alf Laylah Wa Laylah", 38.8956000, -77.4258000),
				new Event("Sully's Restaurant &amp, Supper", 38.9003000, -77.4467000),
				new Event("TGI Friday", 38.8725000, -77.3829000),
				new Event("Potomac Swing Dance Club", 38.9027000, -77.2639000),
				new Event("White Tiger Restaurant", 38.9027000, -77.2638000),
				new Event("Jammin' Java", 38.9039000, -77.2622000),
				new Event("Potomac Swing Dance Club", 38.9027000, -77.2639000),
				new Event("WiseAcres Comedy Club", 38.9248000, -77.2344000),
				new Event("Glen Echo Spanish Ballroom", 38.9691000, -77.1400000),
				new Event("Whitlow's on Wilson", 38.8889000, -77.0926000),
				new Event("Iota Club and Cafe", 38.8890000, -77.0923000),
				new Event("Hilton Washington Embassy Row", 38.9103000, -77.0451000),
				new Event("HorseFeathers, Bar & Grill", 39.01220000000001, -77.3942),
				new Event("Marshall Island Airfield", 7.06, 171.2),
				new Event("Midway Island", 25.7, -171.7),
				new Event("North Pole Way", 55.0, 4.0),

			};
		}

		public static IndexDefinition CreateIndexDefinition()
		{
			// this is how the index looks like in JSON

			var jsonIndexDefinition = @"
{
	""Map"" : ""
		from e in docs.Events
		select new {
		   Tag = \""Event\"",
		   _lat = SpatialIndex.Lat(e.Latitude),
		   _lng = SpatialIndex.Lng(e.Longitude),
		   _tier_2 = SpatialIndex.Tier(2, e.Latitude, e.Longitude),
		   _tier_3 = SpatialIndex.Tier(3, e.Latitude, e.Longitude),
		   _tier_4 = SpatialIndex.Tier(4, e.Latitude, e.Longitude),
		   _tier_5 = SpatialIndex.Tier(5, e.Latitude, e.Longitude),
		   _tier_6 = SpatialIndex.Tier(6, e.Latitude, e.Longitude),
		   _tier_7 = SpatialIndex.Tier(7, e.Latitude, e.Longitude),
		   _tier_8 = SpatialIndex.Tier(8, e.Latitude, e.Longitude),
		   _tier_9 = SpatialIndex.Tier(9, e.Latitude, e.Longitude),
		   _tier_10 = SpatialIndex.Tier(10, e.Latitude, e.Longitude),
		   _tier_11 = SpatialIndex.Tier(11, e.Latitude, e.Longitude),
		   _tier_12 = SpatialIndex.Tier(12, e.Latitude, e.Longitude),
		   _tier_13 = SpatialIndex.Tier(13, e.Latitude, e.Longitude),
		   _tier_14 = SpatialIndex.Tier(14, e.Latitude, e.Longitude),
		   _tier_15 = SpatialIndex.Tier(15, e.Latitude, e.Longitude)	
		}"",
	""Stores"" :{
		   ""latitude"" : ""Yes"",
		   ""longitude"" : ""Yes"",
		   ""_tier_2"" : ""Yes"",
		   ""_tier_3"" : ""Yes"",
		   ""_tier_4"" : ""Yes"",
		   ""_tier_5"" : ""Yes"",
		   ""_tier_6"" : ""Yes"",
		   ""_tier_7"" : ""Yes"",
		   ""_tier_8"" : ""Yes"",
		   ""_tier_9"" : ""Yes"",
		   ""_tier_10"" : ""Yes"",
		   ""_tier_11"" : ""Yes"",
		   ""_tier_12"" : ""Yes"",
		   ""_tier_13"" : ""Yes"",
		   ""_tier_14"" : ""Yes"",
		   ""_tier_15"" : ""Yes""			
		},

	""Indexes"" :{
		   ""Tag"" : ""NotAnalyzed"",
		   ""latitude"" : ""NotAnalyzed"",
		   ""longitude"" : ""NotAnalyzed"",
		   ""_tier_2"" : ""NotAnalyzedNoNorms"",
		   ""_tier_3"" : ""NotAnalyzedNoNorms"",
		   ""_tier_4"" : ""NotAnalyzedNoNorms"",
		   ""_tier_5"" : ""NotAnalyzedNoNorms"",
		   ""_tier_6"" : ""NotAnalyzedNoNorms"",
		   ""_tier_7"" : ""NotAnalyzedNoNorms"",
		   ""_tier_8"" : ""NotAnalyzedNoNorms"",
		   ""_tier_9"" : ""NotAnalyzedNoNorms"",
		   ""_tier_10"" : ""NotAnalyzedNoNorms"",
		   ""_tier_11"" : ""NotAnalyzedNoNorms"",
		   ""_tier_12"" : ""NotAnalyzedNoNorms"",
		   ""_tier_13"" : ""NotAnalyzedNoNorms"",
		   ""_tier_14"" : ""NotAnalyzedNoNorms"",
		   ""_tier_15"" : ""NotAnalyzedNoNorms""
		}
}";

			using (var stringReader = new StringReader(jsonIndexDefinition))
			using (var jsonReader = new JsonTextReader(stringReader))
			{
				return JsonExtensions.CreateDefaultJsonSerializer().Deserialize<IndexDefinition>(jsonReader);
			}
		}
	}
}
