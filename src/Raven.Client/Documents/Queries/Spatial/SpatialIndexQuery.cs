//-----------------------------------------------------------------------
// <copyright file="SpatialIndexQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Globalization;
using Raven.Client.Documents.Indexes.Spatial;

namespace Raven.Client.Documents.Queries.Spatial
{
    /// <summary>
    /// A query using spatial filtering
    /// </summary>
    internal class SpatialIndexQuery : IndexQuery
    {
        public static string GetQueryShapeFromLatLon(double lat, double lng, double radius)
        {
            return "Circle(" +
                   lng.ToString("F6", CultureInfo.InvariantCulture) + " " +
                   lat.ToString("F6", CultureInfo.InvariantCulture) + " " +
                   "d=" + radius.ToString("F6", CultureInfo.InvariantCulture) +
                   ")";
        }

        /// <summary>
        /// Shape in WKT format.
        /// </summary>
        public string QueryShape { get; set; }

        /// <summary>
        /// Spatial relation (Within, Contains, Disjoint, Intersects, Nearby)
        /// </summary>
        public SpatialRelation SpatialRelation { get; set; }

        /// <summary>
        /// A measure of acceptable error of the shape as a fraction. This effectively
        /// inflates the size of the shape but should not shrink it.
        /// </summary>
        /// <value>Default value is 0.025</value>
        public double DistanceErrorPercentage { get; set; }

        /// <summary>
        /// Overrides the units defined in the spatial index
        /// </summary>
        public SpatialUnits? RadiusUnitOverride { get; set; }

        public string SpatialFieldName { get; set; } = Constants.Documents.Indexing.Fields.DefaultSpatialFieldName;

        /// <summary>
        /// Initializes a new instance of the <see cref="SpatialIndexQuery"/> class.
        /// </summary>
        /// <param name="query">The query.</param>
        public SpatialIndexQuery(IndexQuery query)
        {
            Query = query.Query;
            Start = query.Start;
            WaitForNonStaleResultsTimeout = query.WaitForNonStaleResultsTimeout;
            WaitForNonStaleResultsAsOfNow = query.WaitForNonStaleResultsAsOfNow;
            CutoffEtag = query.CutoffEtag;
            PageSize = query.PageSize;
            HighlightedFields = query.HighlightedFields;
            HighlighterPreTags = query.HighlighterPreTags;
            HighlighterPostTags = query.HighlighterPostTags;
            HighlighterKeyName = query.HighlighterKeyName;
            Transformer = query.Transformer;
            TransformerParameters = query.TransformerParameters;
            ExplainScores = query.ExplainScores;
            AllowMultipleIndexEntriesForSameDocumentToResultTransformer =
                query.AllowMultipleIndexEntriesForSameDocumentToResultTransformer;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SpatialIndexQuery"/> class.
        /// </summary>
        public SpatialIndexQuery()
        {
            DistanceErrorPercentage = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct;
        }

        /// <summary>
        /// Gets the custom query string variables.
        /// </summary>
        /// <returns></returns>
        protected override string GetCustomQueryStringVariables()
        {
            var unitsParam = string.Empty;
            if (RadiusUnitOverride.HasValue)
                unitsParam = string.Format("&spatialUnits={0}", RadiusUnitOverride.Value);

            return $"queryShape={Uri.EscapeDataString(QueryShape)}&spatialRelation={SpatialRelation}&spatialField={SpatialFieldName}&distErrPrc={DistanceErrorPercentage.ToString(CultureInfo.InvariantCulture)}{unitsParam}";
        }
    }
}
