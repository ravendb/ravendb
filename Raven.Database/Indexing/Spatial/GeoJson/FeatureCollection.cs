//-----------------------------------------------------------------------
// <copyright file="FeatureCollection.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;

namespace Raven.Database.Indexing.Spatial.GeoJson
{
    public class FeatureCollection
    {
        public FeatureCollection()
        {
            Features = new List<Feature>();
        }

        public FeatureCollection(IEnumerable<Feature> features)
        {
            Features = new List<Feature>(features);
        }

        public FeatureCollection(params Feature[] features)
        {
            Features = new List<Feature>(features);
        }

        public List<Feature> Features { get; private set; }
    }
}
