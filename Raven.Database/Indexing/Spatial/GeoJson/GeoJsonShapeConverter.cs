//-----------------------------------------------------------------------
// <copyright file="GeoJsonShapeConverter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using GeoAPI.Geometries;
using Raven.Json.Linq;
using Spatial4n.Core.Context.Nts;
using Spatial4n.Core.Shapes;
using Spatial4n.Core.Shapes.Nts;

namespace Raven.Database.Indexing.Spatial.GeoJson
{
	public class GeoJsonShapeConverter
	{
		private readonly NtsSpatialContext context;
		private readonly GeoJsonReader geoJsonReader;

		public GeoJsonShapeConverter(NtsSpatialContext context)
		{
			this.context = context;
			geoJsonReader = new GeoJsonReader(context.GetGeometryFactory());
		}

		public bool TryConvert(RavenJObject json, out Shape geometry)
		{
			object obj;
			IGeometry geom;
			if (geoJsonReader.TryRead(json, out obj) && TryConvertInternal(obj, out geom))
			{
				geometry = new NtsGeometry(geom, context, context.IsGeo());
				return true;
			}
			
			geometry = null;
			return false;
		}

		private bool TryConvertInternal(object obj, out IGeometry geometry)
		{
			var geom = obj as IGeometry;
			if (geom != null)
			{
				geometry = geom;
				return true;
			}

			var feature = obj as Feature;
			if (feature != null && feature.Geometry != null)
			{
				geometry = feature.Geometry;
				return true;
			}

			var geoms = new List<IGeometry>();
			var feats = obj as FeatureCollection;
			if (feats != null && feats.Features != null)
			{
				foreach (var feat in feats.Features)
				{
					IGeometry featGeom;
					if (TryConvertInternal(feat.Geometry, out featGeom))
					{
						var collection = featGeom as IGeometryCollection;
						if (collection != null)
							geoms.AddRange(collection.Geometries);
						else
							geoms.Add(featGeom);
					}
					geometry = context.GetGeometryFactory().CreateGeometryCollection(geoms.ToArray());
					return true;
				}
			}

			geometry = null;
			return false;
		}
	}
}