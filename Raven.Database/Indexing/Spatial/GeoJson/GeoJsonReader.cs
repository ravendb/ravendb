//-----------------------------------------------------------------------
// <copyright file="GeoJsonReader.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using GeoAPI.Geometries;
using NetTopologySuite.Geometries;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Database.Indexing.Spatial.GeoJson
{
	public class GeoJsonReader
    {
		// Ported from Geo with permission. http://github.com/sibartlett/Geo
	    private readonly IGeometryFactory geometryFactory;

	    public GeoJsonReader(IGeometryFactory geometryFactory)
	    {
		    this.geometryFactory = geometryFactory;
	    }

		public bool TryRead(RavenJObject json, out object result)
        {
			if (TryParseGeometry(json, out result))
				return true;
			if (TryParseFeature(json, out result))
				return true;
			if (TryParseFeatureCollection(json, out result))
				return true;

			result = null;
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

        private bool TryParseFeatureCollection(RavenJObject obj, out object result)
        {
            result = null;
            string typeString;
	        if (TryParseTypeString(obj, out typeString) && typeString.ToLowerInvariant() == "featurecollection")
	        {
		        RavenJToken feats = null;
				if (obj.TryGetValue("features", out feats))
		        {
					var features = feats as RavenJArray;
			        if (features != null)
			        {
				        var temp = new object[features.Length];
				        for (var index = 0; index < features.Length; index++)
				        {
					        var geometry = features[index];
					        if (!TryParseFeature((RavenJObject) geometry, out temp[index]))
						        return false;
				        }
				        result = new FeatureCollection(temp.Cast<Feature>());
				        return true;
			        }
		        }
	        }
	        return false;
        }

        private bool TryParseFeature(RavenJObject obj, out object result)
        {
            string typeString;
            if(TryParseTypeString(obj, out typeString) && typeString.ToLowerInvariant() == "feature")
            {
				RavenJToken geometry;
                object geo;
                if (obj.TryGetValue("geometry", out geometry) && TryParseGeometry((RavenJObject)geometry, out geo))
                {
					RavenJToken prop;
                    Dictionary<string, object> pr = null;
                    if (obj.TryGetValue("properties", out prop) && prop is RavenJObject)
                    {
                        var props = (RavenJObject) prop;
                        if (props.Count > 0)
                        {
                            pr = props.ToDictionary(x => x.Key, x=> SantizeRavenJObjects(x.Value));
                        }
                    }

                    result = new Feature((IGeometry)geo, pr);

					RavenJToken id;
                    if (obj.TryGetValue("id", out id))
                    {
                        ((Feature) result).Id = SantizeRavenJObjects(id);
                    }

                    return true;
                }
            }
            result = null;
            return false;
        }

        private bool TryParseGeometry(RavenJObject obj, out object result)
        {
            result = null;
            string typeString;
            if (!TryParseTypeString(obj, out typeString))
                return false;

            typeString = typeString.ToLowerInvariant();

            switch (typeString)
            {
                case "point":
                    return TryParsePoint(obj, out result);
                case "linestring":
                    return TryParseLineString(obj, out result);
                case "polygon":
                    return TryParsePolygon(obj, out result);
                case "multipoint":
                    return TryParseMultiPoint(obj, out result);
                case "multilinestring":
                    return TryParseMultiLineString(obj, out result);
                case "multipolygon":
                    return TryParseMultiPolygon(obj, out result);
                case "geometrycollection":
                    return TryParseGeometryCollection(obj, out result);
                default:
                    return false;
            }
        }

        private bool TryParsePoint(RavenJObject obj, out object result)
        {
            result = null;
            RavenJToken coord;
			if (obj.TryGetValue("coordinates", out coord))
			{
				var coordinates = coord as RavenJArray;

				if (coordinates == null || coordinates.Length < 2)
					return false;

				Coordinate coordinate;
				if (TryParseCoordinate(coordinates, out coordinate))
				{
					result = geometryFactory.CreatePoint(coordinate);
					return true;
				}
			}
            return false;
        }

        private bool TryParseLineString(RavenJObject obj, out object result)
        {
            RavenJToken coord;
	        if (obj.TryGetValue("coordinates", out coord))
	        {
				var coordinates = coord as RavenJArray;
				Coordinate[] co;
				if (coordinates != null && TryParseCoordinateArray(coordinates, out co))
				{
					result = geometryFactory.CreateLineString(co);
					return true;
				}
	        }
            result = null;
            return false;
        }

        private bool TryParsePolygon(RavenJObject obj, out object result)
        {
            RavenJToken coord;
	        if (obj.TryGetValue("coordinates", out coord))
	        {
		        var coordinates = coord as RavenJArray;

		        Coordinate[][] temp;
		        if (coordinates != null && coordinates.Length > 0 && TryParseCoordinateArrayArray(coordinates, out temp))
		        {
			        result = geometryFactory.CreatePolygon(
				        geometryFactory.CreateLinearRing(temp[0]),
				        temp.Skip(1).Select(x => geometryFactory.CreateLinearRing(x)).ToArray()
				        );
			        return true;
		        }
	        }
	        result = null;
            return false;
        }

        private bool TryParseMultiPoint(RavenJObject obj, out object result)
        {
            RavenJToken coord;
	        if (obj.TryGetValue("coordinates", out coord))
	        {
				var coordinates = coord as RavenJArray;
				Coordinate[] co;
				if (coordinates != null && TryParseCoordinateArray(coordinates, out co))
				{
					result = geometryFactory.CreateMultiPoint(co);
					return true;
				}
            }
            result = null;
            return false;
        }

        private bool TryParseMultiLineString(RavenJObject obj, out object result)
        {
            RavenJToken coord;
	        if (obj.TryGetValue("coordinates", out coord))
	        {
		        var coordinates = coord as RavenJArray;
		        Coordinate[][] co;
		        if (coordinates != null && TryParseCoordinateArrayArray(coordinates, out co))
		        {
			        result = geometryFactory.CreateMultiLineString(
				        co.Select(x => geometryFactory.CreateLineString(x)).ToArray()
				        );
			        return true;
		        }
	        }
	        result = null;
            return false;
        }

        private bool TryParseMultiPolygon(RavenJObject obj, out object result)
        {
            RavenJToken coord;
	        if (obj.TryGetValue("coordinates", out coord))
	        {
		        var coordinates = coord as RavenJArray;
		        Coordinate[][][] co;
		        if (coordinates != null && TryParseCoordinateArrayArrayArray(coordinates, out co))
		        {
			        result = geometryFactory.CreateMultiPolygon(
				        co.Select(x => geometryFactory.CreatePolygon(
					        geometryFactory.CreateLinearRing(x[0]),
					        x.Skip(1).Select(c => geometryFactory.CreateLinearRing(c)).ToArray()
					                       )
					        ).ToArray()
				        );
			        return true;
		        }
	        }
	        result = null;
            return false;
        }

        private bool TryParseGeometryCollection(RavenJObject obj, out object result)
        {
            result = null;
            RavenJToken geom;
			if (obj.TryGetValue("geometries", out geom))
	        {
				var geometries = geom as RavenJArray;

		        if (geometries != null)
		        {
			        var temp = new object[geometries.Length];
			        for (var index = 0; index < geometries.Length; index++)
			        {
				        var geometry = geometries[index];
				        if (!TryParseGeometry((RavenJObject) geometry, out temp[index]))
					        return false;
			        }
			        result = new GeometryCollection(temp.Cast<IGeometry>().ToArray());
			        return true;
		        }
	        }
	        return false;
        }

        private bool TryParseCoordinate(RavenJArray coordinates, out Coordinate result)
        {
			if (coordinates != null && coordinates.Length > 1 && coordinates.All(x => x is RavenJValue))
			{
				var vals = coordinates.Cast<RavenJValue>().ToList();
				if (vals.All(x => x.Type == JTokenType.Float || x.Type == JTokenType.Integer))
				{
					result = new Coordinate(Convert.ToDouble(vals[0].Value), Convert.ToDouble(vals[1].Value));
					return true;
				}
			}
			result = null;
			return false;
        }

        private bool TryParseCoordinateArray(RavenJArray coordinates, out Coordinate[] result)
        {
            result = null;
            if (coordinates == null)
                return false;

            var valid = coordinates.All(x => x is RavenJArray);
            if (!valid)
                return false;

			var tempResult = new Coordinate[coordinates.Length];
			for (var index = 0; index < coordinates.Length; index++)
            {
                if (!TryParseCoordinate((RavenJArray)coordinates[index], out tempResult[index]))
                    return false;
            }
            result = tempResult;
            return true;
        }

        private bool TryParseCoordinateArrayArray(RavenJArray coordinates, out Coordinate[][] result)
        {
            result = null;
            if (coordinates == null)
                return false;

            var valid = coordinates.All(x => x is RavenJArray);
            if (!valid)
                return false;

			var tempResult = new Coordinate[coordinates.Length][];
			for (var index = 0; index < coordinates.Length; index++)
            {
                if (!TryParseCoordinateArray((RavenJArray)coordinates[index], out tempResult[index]))
                    return false;
            }
            result = tempResult;
            return true;
        }

        private bool TryParseCoordinateArrayArrayArray(RavenJArray coordinates, out Coordinate[][][] result)
        {
            result = null;
            if (coordinates == null)
                return false;

            var valid = coordinates.All(x => x is RavenJArray);
            if (!valid)
                return false;

			var tempResult = new Coordinate[coordinates.Length][][];
			for (var index = 0; index < coordinates.Length; index++)
            {
                if (!TryParseCoordinateArrayArray((RavenJArray)coordinates[index], out tempResult[index]))
                    return false;
            }
            result = tempResult;
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
