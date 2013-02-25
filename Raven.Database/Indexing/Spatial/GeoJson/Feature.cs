//-----------------------------------------------------------------------
// <copyright file="AbstractViewGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using GeoAPI.Geometries;

namespace Raven.Database.Indexing.Spatial.GeoJson
{
    public class Feature
    {
	    public Feature(IGeometry geometry, Dictionary<string, object> properties = null)
        {
            Geometry = geometry;
            Properties = properties ?? new Dictionary<string, object>();
        }

		public IGeometry Geometry { get; set; }
        public Dictionary<string, object> Properties { get; set; }
		public object Id { get; set; }
    }
}
