//-----------------------------------------------------------------------
// <copyright file="GeoJsonReader.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Globalization;
using System.Linq;
using System.Text;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Abstractions.Spatial
{
	public class GeoJsonWktConverter
    {
		public bool TryConvert(RavenJObject json, out string result)
		{
			var builder = new StringBuilder();

			if (TryParseGeometry(json, builder))
			{
				result = builder.ToString();
				return true;
			}
			if (TryParseFeature(json, builder))
			{
				result = builder.ToString();
				return true;
			}
			//if (TryParseFeatureCollection(json, out result))
			//	return true;

			result = default(string);
            return false;
        }

        private bool TryParseTypeString(RavenJObject obj, out string result)
        {
			RavenJToken type = null;
            if (obj != null)
                obj.TryGetValue("type", out type);
                
            result = ((RavenJValue) type).Value as string;
			
            return type != null;
        }

		//private bool TryParseFeatureCollection(RavenJObject obj, StringBuilder result)
		//{
		//	result = null;
		//	string typeString;
		//	if (TryParseTypeString(obj, out typeString) && typeString.ToLowerInvariant() == "featurecollection")
		//	{
		//		RavenJToken feats = null;
		//		if (obj.TryGetValue("features", out feats))
		//		{
		//			var features = feats as RavenJArray;
		//			if (features != null)
		//			{
		//				var temp = new object[features.Length];
		//				for (var index = 0; index < features.Length; index++)
		//				{
		//					var geometry = features[index];
		//					if (!TryParseFeature((RavenJObject) geometry, out temp[index]))
		//						return false;
		//				}
		//				result = new FeatureCollection(temp.Cast<Feature>());
		//				return true;
		//			}
		//		}
		//	}
		//	return false;
		//}

		private bool TryParseFeature(RavenJObject obj, StringBuilder builder)
		{
			string typeString;
			if (TryParseTypeString(obj, out typeString) && typeString.ToLowerInvariant() == "feature")
			{
				RavenJToken geometry;
				if (obj.TryGetValue("geometry", out geometry) && TryParseGeometry((RavenJObject) geometry, builder))
					return true;
			}
			return false;
		}

        private bool TryParseGeometry(RavenJObject obj, StringBuilder builder)
        {
            string typeString;
            if (!TryParseTypeString(obj, out typeString))
                return false;

            typeString = typeString.ToLowerInvariant();

            switch (typeString)
            {
                case "point":
                    return TryParsePoint(obj, builder);
                case "linestring":
					return TryParseLineString(obj, builder);
                case "polygon":
					return TryParsePolygon(obj, builder);
                case "multipoint":
                    return TryParseMultiPoint(obj, builder);
                case "multilinestring":
                    return TryParseMultiLineString(obj, builder);
                case "multipolygon":
                    return TryParseMultiPolygon(obj, builder);
                case "geometrycollection":
                    return TryParseGeometryCollection(obj, builder);
                default:
                    return false;
            }
        }

        private bool TryParsePoint(RavenJObject obj, StringBuilder builder)
        {
            RavenJToken coord;
			if (obj.TryGetValue("coordinates", out coord))
			{
				var coordinates = coord as RavenJArray;

				if (coordinates == null || coordinates.Length < 2)
					return false;

				builder.Append("POINT (");
				if (TryParseCoordinate(coordinates, builder))
				{
					builder.Append(")");
					return true;
				}
			}
            return false;
        }

        private bool TryParseLineString(RavenJObject obj, StringBuilder builder)
        {
            RavenJToken coord;
	        if (obj.TryGetValue("coordinates", out coord))
	        {
				var coordinates = coord as RavenJArray;
				builder.Append("LINESTRING (");
				if (coordinates != null && TryParseCoordinateArray(coordinates, builder))
				{
					builder.Append(")");
					return true;
				}
	        }
            return false;
        }

        private bool TryParsePolygon(RavenJObject obj, StringBuilder builder)
        {
            RavenJToken coord;
	        if (obj.TryGetValue("coordinates", out coord))
	        {
		        var coordinates = coord as RavenJArray;

				builder.Append("POLYGON (");
				if (coordinates != null && coordinates.Length > 0 && TryParseCoordinateArrayArray(coordinates, builder))
				{
					builder.Append(")");
					return true;
				}
	        }
            return false;
        }

        private bool TryParseMultiPoint(RavenJObject obj, StringBuilder builder)
        {
            RavenJToken coord;
	        if (obj.TryGetValue("coordinates", out coord))
	        {
				var coordinates = coord as RavenJArray;
				builder.Append("MULTIPOINT (");
				if (coordinates != null && TryParseCoordinateArray(coordinates, builder))
				{
					builder.Append(")");
					return true;
				}
	        }
            return false;
        }

        private bool TryParseMultiLineString(RavenJObject obj, StringBuilder builder)
        {
            RavenJToken coord;
	        if (obj.TryGetValue("coordinates", out coord))
	        {
		        var coordinates = coord as RavenJArray;
				builder.Append("MULTILINESTRING (");
				if (coordinates != null && TryParseCoordinateArrayArray(coordinates, builder))
				{
					builder.Append(")");
					return true;
				}
	        }
            return false;
        }

        private bool TryParseMultiPolygon(RavenJObject obj, StringBuilder builder)
        {
            RavenJToken coord;
	        if (obj.TryGetValue("coordinates", out coord))
	        {
				var coordinates = coord as RavenJArray;
				builder.Append("MULTIPOLYGON (");
				if (coordinates != null && TryParseCoordinateArrayArrayArray(coordinates, builder))
				{
					builder.Append(")");
					return true;
				}
	        }
            return false;
        }

        private bool TryParseGeometryCollection(RavenJObject obj, StringBuilder builder)
        {
            RavenJToken geom;
			if (obj.TryGetValue("geometries", out geom))
	        {
				var geometries = geom as RavenJArray;

		        if (geometries != null)
		        {
					builder.Append("GEOMETRYCOLLECTION (");
			        for (var index = 0; index < geometries.Length; index++)
					{
						if (index > 0)
							builder.Append(", ");
				        var geometry = geometries[index];
						if (!TryParseGeometry((RavenJObject)geometry, builder))
					        return false;
			        }
					builder.Append(")");
			        return true;
		        }
	        }
	        return false;
        }

        private bool TryParseCoordinate(RavenJArray coordinates, StringBuilder result)
        {
			if (coordinates != null && coordinates.Length > 1 && coordinates.All(x => x is RavenJValue))
			{
				var vals = coordinates.Cast<RavenJValue>().ToList();
				if (vals.All(x => x.Type == JTokenType.Float || x.Type == JTokenType.Integer))
				{
					result.AppendFormat(
						CultureInfo.InvariantCulture,
						"{0} {1}",
						Convert.ToDouble(vals[0].Value),
					    Convert.ToDouble(vals[1].Value)
					);
					return true;
				}
			}
			return false;
        }

        private bool TryParseCoordinateArray(RavenJArray coordinates, StringBuilder result)
        {
            if (coordinates == null)
                return false;

            var valid = coordinates.All(x => x is RavenJArray);
            if (!valid)
                return false;

			for (var index = 0; index < coordinates.Length; index++)
			{
				if (index > 0)
					result.Append(", ");
				if (!TryParseCoordinate((RavenJArray)coordinates[index], result))
                    return false;
            }
            return true;
        }

        private bool TryParseCoordinateArrayArray(RavenJArray coordinates, StringBuilder result)
        {
            if (coordinates == null)
                return false;

            var valid = coordinates.All(x => x is RavenJArray);
            if (!valid)
                return false;

			for (var index = 0; index < coordinates.Length; index++)
			{
				if (index > 0)
					result.Append(", ");
				result.Append("(");
				if (!TryParseCoordinateArray((RavenJArray)coordinates[index], result))
					return false;
				result.Append(")");
            }
            return true;
        }

        private bool TryParseCoordinateArrayArrayArray(RavenJArray coordinates, StringBuilder result)
        {
            if (coordinates == null)
                return false;

            var valid = coordinates.All(x => x is RavenJArray);
            if (!valid)
                return false;

			for (var index = 0; index < coordinates.Length; index++)
			{
				if (index > 0)
					result.Append(", ");
				result.Append("(");
				if (!TryParseCoordinateArrayArray((RavenJArray)coordinates[index], result))
					return false;
				result.Append(")");
            }
            return true;
        }

        public object SantizeRavenJObjects(object obj)
        {
            var ravenJArray = obj as RavenJArray;
            if (ravenJArray != null)
                return ravenJArray.Select(SantizeRavenJObjects).ToArray();

            var ravenJObject = obj as RavenJObject;
            if (ravenJObject != null)
                return ravenJObject.ToDictionary(x => x.Key, x => SantizeRavenJObjects(x));

            return obj;
        }
    }
}
