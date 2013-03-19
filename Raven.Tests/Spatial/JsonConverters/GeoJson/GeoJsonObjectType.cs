// --------------------------------------------------------------------------------------------------------------------
// <copyright file="GeoJSONObjectType.cs" company="Jörg Battermann">
//   Copyright © Jörg Battermann 2011
// </copyright>
// <summary>
//   Defines the GeoJSON Objects types as defined in the <see cref="http://geojson.org/geojson-spec.html#geojson-objects">geojson.org v1.0 spec</see>.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

// From: https://code.google.com/p/nettopologysuite/source/browse/#svn%2Ftrunk%2FNetTopologySuite.IO%2FNetTopologySuite.IO.GeoJSON
namespace Raven.Tests.Spatial.JsonConverters.GeoJson
{
    /// <summary>
    /// Defines the GeoJSON Objects types as defined in the <see cref="http://geojson.org/geojson-spec.html#geojson-objects">geojson.org v1.0 spec</see>.
    /// </summary>
    public enum GeoJsonObjectType
    {
        /// <summary>
        /// Defines the <see cref="http://geojson.org/geojson-spec.html#point">Point</see> type.
        /// </summary>
        Point,

        /// <summary>
        /// Defines the <see cref="http://geojson.org/geojson-spec.html#multipoint">MultiPoint</see> type.
        /// </summary>
        MultiPoint,

        /// <summary>
        /// Defines the <see cref="http://geojson.org/geojson-spec.html#linestring">LineString</see> type.
        /// </summary>
        LineString,

        /// <summary>
        /// Defines the <see cref="http://geojson.org/geojson-spec.html#multilinestring">MultiLineString</see> type.
        /// </summary>
        MultiLineString,

        /// <summary>
        /// Defines the <see cref="http://geojson.org/geojson-spec.html#polygon">Polygon</see> type.
        /// </summary>
        Polygon,

        /// <summary>
        /// Defines the <see cref="http://geojson.org/geojson-spec.html#multipolygon">MultiPolygon</see> type.
        /// </summary>
        MultiPolygon,

        /// <summary>
        /// Defines the <see cref="http://geojson.org/geojson-spec.html#geometry-collection">GeometryCollection</see> type.
        /// </summary>
        GeometryCollection,

        /// <summary>
        /// Defines the <see cref="http://geojson.org/geojson-spec.html#feature-objects">Feature</see> type.
        /// </summary>
        Feature,

        /// <summary>
        /// Defines the <see cref="http://geojson.org/geojson-spec.html#feature-collection-objects">FeatureCollection</see> type.
        /// </summary>
        FeatureCollection
    }
}
