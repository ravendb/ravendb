//-----------------------------------------------------------------------
// <copyright file="SpatialDocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Data;
#if !SILVERLIGHT


namespace Raven.Client.Document
{
    /// <summary>
    /// A spatial query allows to perform spatial filtering on the index
    /// </summary>
    public class SpatialDocumentQuery<T> : DocumentQuery<T>
    {
        private readonly double lat, lng, radius;

        /// <summary>
        /// Initializes a new instance of the <see cref="SpatialDocumentQuery&lt;T&gt;"/> class.
        /// </summary>
        /// <param name="documentQuery">The document query.</param>
        /// <param name="radius">The radius.</param>
        /// <param name="latitude">The latitude.</param>
        /// <param name="longitude">The longitude.</param>
        public SpatialDocumentQuery(DocumentQuery<T> documentQuery, double radius, double latitude, double longitude)
            : base(documentQuery)
        {
            this.radius = radius;
            lat = latitude;
            lng = longitude;
        }

    	/// <summary>
    	/// Initializes a new instance of the <see cref="SpatialDocumentQuery&lt;T&gt;"/> class.
    	/// </summary>
    	/// <param name="documentQuery">The document query.</param>
    	public SpatialDocumentQuery(DocumentQuery<T> documentQuery)
            : base(documentQuery)
        {
            var other = documentQuery as SpatialDocumentQuery<T>;
            if (other == null)
                return;

            radius = other.radius;
            lat = other.lat;
            lng = other.lng;
        }


        /// <summary>
        /// Generates the index query.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        protected override IndexQuery GenerateIndexQuery(string query)
        {
        	var generateIndexQuery = new SpatialIndexQuery
        	{
        		Query = query,
        		Start = start,
        		Cutoff = cutoff,
        		SortedFields = orderByFields.Select(x => new SortedField(x)).ToArray(),
        		FieldsToFetch = projectionFields,
        		Latitude = lat,
        		Longitude = lng,
        		Radius = radius,
        	};
			if (pageSize != null)
				generateIndexQuery.PageSize = pageSize.Value;
        	return generateIndexQuery;
        }
    }
}

#endif
