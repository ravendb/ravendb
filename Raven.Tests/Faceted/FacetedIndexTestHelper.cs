//-----------------------------------------------------------------------
// <copyright file="SpatialIndexTestHelper.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.IO;
using Newtonsoft.Json;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Raven.Database.Json;
using System.Collections.Generic;
using System;
using System.Linq;

namespace Raven.Tests.Faceted
{
    public static class FacetedIndexTestHelper
	{
        private static List<string> Features = new List<string> 
                    { 
                        "Image Stabiliser", 
                        "Tripod",
                        "Low Light Compatible",
                        "Fixed Lens",
                        "LCD"
                    };

        private static List<string> Manufacturers = new List<string> 
                    { 
                        "Sony", 
                        "Nikon",
                        "Phillips",
                        "Canon",
                        "Jessops"
                    };

        private static Random random = new Random(1337);

        public static IList<Camera> GetCameras(int numCameras)
		{
            var cameraList = new List<Camera>(numCameras);

            for (int i = 1; i <= numCameras; i++)
            {
                cameraList.Add(new Camera
                {
                    Id = i,
                    DateOfListing = new DateTime(1980 + random.Next(1, 30), random.Next(1, 12), random.Next(1, 27)),
                    Manufacturer = Manufacturers[(int)(random.NextDouble() * Manufacturers.Count)],
                    Model = "blah",
                    Cost = (decimal)((random.NextDouble() * 900.0) + 100.0),
                    Zoom = (int)(random.NextDouble() * 12) + 2,
                    Megapixels = (decimal)(random.NextDouble() * 9.0) + 1.0m,
                    ImageStabiliser = random.NextDouble() > 0.6,
                    AdvancedFeatures = new List<string> { "??" }
                });
            }

            return cameraList;           
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
		   _lng = SpatialIndex.Lng(e.Longitude)		   
		}"",
	""Stores"" :{
		   ""latitude"" : ""Yes"",
		   ""longitude"" : ""Yes""		   
		},

	""Indexes"" :{
		   ""Tag"" : ""NotAnalyzed"",
		   ""latitude"" : ""NotAnalyzed"",
		   ""longitude"" : ""NotAnalyzed""		   
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
